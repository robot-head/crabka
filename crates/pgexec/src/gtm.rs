//! Range 0's Global Transaction Manager. It allocates monotonic GLOBAL xids,
//! which are `>= GLOBAL_XID_BASE` and disjoint from every range's local xids. It
//! tracks the in-flight global set. It builds the global snapshot a cross-range
//! reader resolves Prepared(->G) tuples against. Range 0's store backs it, and
//! the state machine max-merges the counter exactly as it does ProcArray's
//! next_xid.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use crabka_pgkv::Kv;
use crabka_pgmvcc::{visibility::Snapshot, xid::GLOBAL_XID_BASE};
use zerocopy::{FromBytes, IntoBytes, byteorder::big_endian::U64};

use crate::error::ExecError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalXidLease {
    next: u64,
    end: u64,
}

impl GlobalXidLease {
    #[must_use]
    pub fn allocate(&mut self) -> Option<u64> {
        if self.next >= self.end {
            return None;
        }
        let xid = self.next;
        self.next += 1;
        Some(xid)
    }

    #[must_use]
    pub fn start(&self) -> u64 {
        self.next
    }

    #[must_use]
    pub fn end(&self) -> u64 {
        self.end
    }
}

struct Inner {
    next_global: u64,
    running: BTreeSet<u64>,
}

pub(crate) struct Gtm {
    inner: Mutex<Inner>,
    kv: Arc<dyn Kv>,
}

/// Decode range 0's durable `next_global_xid` counter. `next_global_xid_op`
/// writes it BIG-ENDIAN, as `U64::new(next).as_bytes()`, so this function MUST
/// read it big-endian. A native-endian or little-endian decode mis-reads the
/// real allocator's bytes and clamps every reader's global horizon to
/// `GLOBAL_XID_BASE`, which hides every committed cross-range row in the wired
/// path. That is correction C1. There is ONE decode site, shared by `Gtm::open`
/// and `session::durable_global_snapshot`. An absent counter reads as
/// `GLOBAL_XID_BASE`, and the value never regresses below the base.
pub(crate) fn read_next_global(kv: &dyn Kv) -> Result<u64, ExecError> {
    let next = match kv.get(&crabka_pgkv::key::meta_next_global_xid_key())? {
        Some(b) => {
            let (v, _) = U64::read_from_prefix(b.as_slice())
                .map_err(|_| crabka_pgkv::KvError::CorruptRow("next_global_xid not u64".into()))?;
            v.get()
        }
        None => GLOBAL_XID_BASE,
    };
    Ok(next.max(GLOBAL_XID_BASE))
}

impl Gtm {
    pub fn open(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let next = read_next_global(kv.as_ref())?;
        Ok(Self {
            inner: Mutex::new(Inner {
                next_global: next,
                running: BTreeSet::new(),
            }),
            kv,
        })
    }

    pub fn begin_global(&self) -> u64 {
        let mut g = self.inner.lock().expect("gtm");
        let xid = g.next_global;
        g.next_global = xid + 1;
        g.running.insert(xid);
        xid
    }

    pub fn lease_global_block(&self, count: u64) -> Result<GlobalXidLease, ExecError> {
        if count == 0 {
            return Err(ExecError::Unsupported(
                "global xid lease size must be greater than zero".into(),
            ));
        }
        let mut g = self.inner.lock().expect("gtm");
        let start = g.next_global;
        let end = start.checked_add(count).ok_or_else(|| {
            crabka_pgkv::KvError::CorruptRow("global xid lease overflows u64".into())
        })?;
        g.next_global = end;
        Ok(GlobalXidLease { next: start, end })
    }

    pub fn next_global_xid_op(&self) -> crabka_pgkv::WriteOp {
        let next = self.inner.lock().expect("gtm").next_global;
        crabka_pgkv::WriteOp::Put {
            key: crabka_pgkv::key::meta_next_global_xid_key(),
            value: U64::new(next).as_bytes().to_vec(),
        }
    }

    pub fn reseed_from_applied(&self) -> Result<(), ExecError> {
        let durable = match self.kv.get(&crabka_pgkv::key::meta_next_global_xid_key())? {
            Some(b) => {
                let (v, _) = U64::read_from_prefix(b.as_slice()).map_err(|_| {
                    crabka_pgkv::KvError::CorruptRow("next_global_xid not u64".into())
                })?;
                v.get()
            }
            None => GLOBAL_XID_BASE,
        };
        let mut g = self.inner.lock().expect("gtm");
        g.next_global = g.next_global.max(durable.max(GLOBAL_XID_BASE));
        Ok(())
    }

    /// ONLY `global_status` consumes this. Nothing ever hands it to
    /// satisfies_mvcc. `xip` is BTreeSet-sorted for the resolver's
    /// binary_search, and `xmin` is unused.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn global_snapshot(&self) -> Snapshot {
        let g = self.inner.lock().expect("gtm");
        let xip: Vec<u64> = g.running.iter().copied().collect();
        let xmax = g.next_global;
        Snapshot {
            xmin: xip.first().copied().unwrap_or(xmax),
            xmax,
            xip,
        }
    }

    pub fn finish_global(&self, g: u64) {
        self.inner.lock().expect("gtm").running.remove(&g);
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgkv::MemKv;

    use super::*;

    #[test]
    fn allocates_disjoint_monotonic_global_ids() {
        let gtm = Gtm::open(Arc::new(MemKv::new())).expect("open");
        let (a, b) = (gtm.begin_global(), gtm.begin_global());
        assert!(a >= GLOBAL_XID_BASE && b == a + 1);
        assert_eq!(gtm.global_snapshot().xip, vec![a, b]);
        gtm.finish_global(a);
        assert_eq!(gtm.global_snapshot().xip, vec![b]);
    }

    #[test]
    fn reseed_lifts_counter_and_never_regresses() {
        let kv = Arc::new(MemKv::new());
        let gtm = Gtm::open(kv.clone() as Arc<dyn Kv>).expect("open");
        assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE);
        kv.put(
            crabka_pgkv::key::meta_next_global_xid_key(),
            (GLOBAL_XID_BASE + 50).to_be_bytes().to_vec(),
        )
        .expect("put");
        gtm.reseed_from_applied().expect("reseed");
        assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE + 50);
    }

    #[test]
    fn stale_in_memory_counter_reuses_g_until_reseed() {
        let kv = std::sync::Arc::new(MemKv::new());
        let gtm = Gtm::open(kv.clone() as Arc<dyn Kv>).expect("open"); // in-memory next_global == BASE
        // A PRIOR leader durably allocated through BASE+4 (begin_global_durable committed next=BASE+5).
        kv.put(
            crabka_pgkv::key::meta_next_global_xid_key(),
            (GLOBAL_XID_BASE + 5).to_be_bytes().to_vec(),
        )
        .expect("put");
        // TEETH: a new leader that does NOT reseed re-hands-out BASE (already allocated by the prior leader).
        assert_eq!(
            gtm.begin_global(),
            GLOBAL_XID_BASE,
            "without reseed, the stale counter reuses g (the bug)"
        );
        gtm.finish_global(GLOBAL_XID_BASE);
        // POSITIVE: after reseed_from_applied (durable value current), the next g is past every allocation.
        gtm.reseed_from_applied().expect("reseed");
        assert!(
            gtm.begin_global() >= GLOBAL_XID_BASE + 5,
            "after reseed, g is never reused"
        );
    }
}
