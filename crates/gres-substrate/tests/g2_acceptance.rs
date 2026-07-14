//! G-2 substrate WAL acceptance proof tests over the public substrate seams.

use std::sync::Arc;

use assert2::assert;
use crabka_gres_substrate::{
    InMemoryWalLog, SubstrateCommitter, WalFrame, WriterGeneration, recover_after_barrier,
};
use crabka_pgexec::Committer;
use crabka_pgkv::{Kv, MemKv, WriteOp};

#[tokio::test]
async fn disposability_rebuilds_cache_from_committed_wal() {
    let log = InMemoryWalLog::shared();
    let first_cache: Arc<dyn Kv> = Arc::new(MemKv::default());
    let first_compute =
        substrate_committer(first_cache.clone(), log.clone(), WriterGeneration(0), 0);

    first_compute
        .commit(vec![put("table/row/1", "acked")])
        .await
        .expect("commit acked state");
    drop(first_compute);
    drop(first_cache);

    let rebuilt_cache = MemKv::default();
    let (_barrier, outcome) = recover_after_barrier(&rebuilt_cache, log.as_ref(), log.as_ref())
        .await
        .expect("recover committed WAL");

    assert!(outcome.next_journal_seq == 1);
    assert!(rebuilt_cache.get(b"table/row/1").expect("get") == Some(b"acked".to_vec()));
}

#[tokio::test]
async fn stale_writer_cannot_commit_after_newer_generation_fences_it() {
    let log = InMemoryWalLog::shared();
    let stale_cache: Arc<dyn Kv> = Arc::new(MemKv::default());
    let stale_compute =
        substrate_committer(stale_cache.clone(), log.clone(), WriterGeneration(0), 0);
    stale_compute
        .commit(vec![put("table/row/1", "before-fence")])
        .await
        .expect("initial stale-generation commit");

    let successor_cache = MemKv::default();
    let (barrier, outcome) = recover_after_barrier(&successor_cache, log.as_ref(), log.as_ref())
        .await
        .expect("successor recovery fences stale generation");
    let successor: Arc<dyn Kv> = Arc::new(successor_cache);
    let successor_compute = substrate_committer(
        successor.clone(),
        log.clone(),
        barrier.generation,
        outcome.next_journal_seq,
    );

    successor_compute
        .commit(vec![put("table/row/2", "after-fence")])
        .await
        .expect("successor commit");
    let stale_error = stale_compute
        .commit(vec![put("table/row/3", "stale-write")])
        .await
        .expect_err("stale compute fenced");

    assert!(matches!(stale_error, crabka_pgexec::ExecError::NotLeader));
    assert!(stale_cache.get(b"table/row/3").expect("get").is_none());
    assert!(successor.get(b"table/row/1").expect("get") == Some(b"before-fence".to_vec()));
    assert!(successor.get(b"table/row/2").expect("get") == Some(b"after-fence".to_vec()));
}

#[tokio::test]
async fn oversized_batch_chunks_and_recovers_atomically() {
    let log = InMemoryWalLog::shared();
    let first_cache: Arc<dyn Kv> = Arc::new(MemKv::default());
    let large_value = vec![b'x'; 96];
    let ops = vec![
        WriteOp::Put {
            key: b"chunked/row/1".to_vec(),
            value: b"small".to_vec(),
        },
        WriteOp::Put {
            key: b"chunked/row/2".to_vec(),
            value: large_value.clone(),
        },
        WriteOp::Put {
            key: b"chunked/row/3".to_vec(),
            value: b"small".to_vec(),
        },
    ];
    let frames = crabka_gres_substrate::chunk_wal_batch(ops.clone(), 0, 48).expect("chunk batch");
    let compute = substrate_committer(first_cache, log.clone(), WriterGeneration(0), 0)
        .with_max_frame_bytes(48);

    compute.commit(ops).await.expect("commit chunked group");
    drop(compute);

    let rebuilt_cache = MemKv::default();
    let (_barrier, outcome) = recover_after_barrier(&rebuilt_cache, log.as_ref(), log.as_ref())
        .await
        .expect("recover chunked group");

    assert!(frames.len() == 3);
    assert!(frames[1].encoded_len() > 48);
    assert!(outcome.next_journal_seq == 3);
    assert!(rebuilt_cache.get(b"chunked/row/1").expect("get") == Some(b"small".to_vec()));
    assert!(rebuilt_cache.get(b"chunked/row/2").expect("get") == Some(large_value));
    assert!(rebuilt_cache.get(b"chunked/row/3").expect("get") == Some(b"small".to_vec()));
}

#[tokio::test]
async fn recovery_never_resurrects_unacked_records() {
    let log = InMemoryWalLog::shared();
    let cache: Arc<dyn Kv> = Arc::new(MemKv::default());
    let compute = substrate_committer(cache, log.clone(), WriterGeneration(0), 0);
    compute
        .commit(vec![put("txn/acked", "visible")])
        .await
        .expect("commit acked record");
    log.append_unacked(WriterGeneration(0), &[frame(1, "txn/unacked", "hidden")])
        .await
        .expect("append unacked record");

    let rebuilt_cache = MemKv::default();
    let (_barrier, outcome) = recover_after_barrier(&rebuilt_cache, log.as_ref(), log.as_ref())
        .await
        .expect("recover committed-only WAL");

    assert!(outcome.next_journal_seq == 1);
    assert!(rebuilt_cache.get(b"txn/acked").expect("get") == Some(b"visible".to_vec()));
    assert!(rebuilt_cache.get(b"txn/unacked").expect("get").is_none());
}

fn substrate_committer(
    kv: Arc<dyn Kv>,
    log: Arc<InMemoryWalLog>,
    generation: WriterGeneration,
    next_journal_seq: u64,
) -> SubstrateCommitter<InMemoryWalLog> {
    SubstrateCommitter::new(kv, log, generation, next_journal_seq)
}

fn frame(journal_seq: u64, key: &str, value: &str) -> WalFrame {
    WalFrame {
        journal_seq,
        ops: vec![put(key, value)],
    }
}

fn put(key: &str, value: &str) -> WriteOp {
    WriteOp::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}
