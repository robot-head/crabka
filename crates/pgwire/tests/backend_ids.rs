//! The backend id announced in `BackendKeyData` is not only a session label: it
//! names `pg_temp_<backend id>` in a catalog every gateway of a cluster shares.
//! So it has to be a cluster-wide name, while staying the `int4` a client sends
//! back in a `CancelRequest`.

use std::collections::HashSet;

use assert2::assert;
use crabka_pgwire::server::next_backend_pid;

/// How many ids a case draws — enough that a shared prefix or a repeated value
/// would show up.
const DRAWN: usize = 4096;

/// The lowest id this process can announce. A bare per-process counter starts
/// at 1 in every process, so every gateway of a cluster would name the same
/// handful of temporary namespaces; ids above this range cannot.
const SMALLEST_COUNTER_ONLY_ID: i32 = 1 << 15;

#[test]
fn a_process_announces_positive_int4_ids_no_two_sessions_share() {
    let ids = (0..DRAWN).map(|_| next_backend_pid()).collect::<Vec<_>>();

    assert!(ids.iter().copied().collect::<HashSet<i32>>().len() == ids.len());
    assert!(ids.iter().all(|id| *id > 0));
}

#[test]
fn no_announced_id_lands_where_a_bare_per_process_counter_would() {
    assert!((0..DRAWN).all(|_| next_backend_pid() >= SMALLEST_COUNTER_ONLY_ID));
}
