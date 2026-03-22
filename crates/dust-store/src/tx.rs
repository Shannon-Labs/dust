use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxId(pub u64);

#[derive(Debug)]
pub struct TransactionManager {
    next_tx_id: AtomicU64,
    committed_watermark: AtomicU64,
    active_readers: std::sync::RwLock<HashSet<TxId>>,
    committed_txs: std::sync::RwLock<HashSet<TxId>>,
    /// For each committed write transaction, the set of page IDs it modified.
    /// Used for conflict detection during commit validation.
    committed_write_sets: std::sync::RwLock<HashMap<TxId, HashSet<u64>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            committed_watermark: AtomicU64::new(0),
            active_readers: std::sync::RwLock::new(HashSet::new()),
            committed_txs: std::sync::RwLock::new(HashSet::new()),
            committed_write_sets: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub fn begin_read(&self) -> TxId {
        let tx_id = TxId(self.next_tx_id.fetch_add(1, Ordering::SeqCst));
        self.active_readers.write().unwrap().insert(tx_id);
        tx_id
    }

    pub fn begin_write(&self) -> TxId {
        TxId(self.next_tx_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Record a committed write transaction along with the set of page IDs
    /// it modified. This information is used by subsequent transactions to
    /// detect read-write conflicts.
    pub fn commit_write(&self, tx_id: TxId, written_pages: HashSet<u64>) {
        self.committed_write_sets
            .write()
            .unwrap()
            .insert(tx_id, written_pages);
        self.committed_txs.write().unwrap().insert(tx_id);
        self.active_readers.write().unwrap().remove(&tx_id);
        let mut watermark = self.committed_watermark.load(Ordering::SeqCst);
        loop {
            let current = watermark;
            if current < tx_id.0 {
                match self.committed_watermark.compare_exchange_weak(
                    current,
                    tx_id.0,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(v) => watermark = v,
                }
            } else {
                break;
            }
        }
    }

    pub fn end_read(&self, tx_id: TxId) {
        self.active_readers.write().unwrap().remove(&tx_id);
    }

    pub fn abort(&self, tx_id: TxId) {
        self.active_readers.write().unwrap().remove(&tx_id);
        self.committed_txs.write().unwrap().remove(&tx_id);
        self.committed_write_sets.write().unwrap().remove(&tx_id);
    }

    pub fn is_visible(&self, write_tx_id: TxId, reader_tx_id: TxId) -> bool {
        if write_tx_id == reader_tx_id {
            return true;
        }
        let committed = self.committed_txs.read().unwrap();
        if !committed.contains(&write_tx_id) {
            return false;
        }
        write_tx_id.0 <= reader_tx_id.0
    }

    pub fn has_committed_after(&self, tx_id: TxId) -> bool {
        self.committed_watermark.load(Ordering::SeqCst) > tx_id.0
    }

    /// Check whether any transaction committed after `since_tx` has written
    /// to any page in `read_pages`. Returns true if there is a conflict.
    pub fn has_conflicting_writes(&self, since_tx: TxId, read_pages: &HashSet<u64>) -> bool {
        if read_pages.is_empty() {
            return false;
        }
        let write_sets = self.committed_write_sets.read().unwrap();
        for (tx_id, pages) in write_sets.iter() {
            // Only consider transactions that committed after our snapshot.
            if tx_id.0 > since_tx.0 && !pages.is_disjoint(read_pages) {
                return true;
            }
        }
        false
    }

    /// Prune committed write-set records for transactions older than the
    /// oldest active reader. Called periodically to avoid unbounded growth.
    pub fn gc_write_sets(&self) {
        let readers = self.active_readers.read().unwrap();
        let min_active = readers.iter().map(|t| t.0).min().unwrap_or(u64::MAX);
        drop(readers);
        self.committed_write_sets
            .write()
            .unwrap()
            .retain(|tx_id, _| tx_id.0 >= min_active);
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_ids_are_unique_and_increasing() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin_read();
        let t2 = mgr.begin_write();
        let t3 = mgr.begin_read();
        assert!(t1.0 < t2.0);
        assert!(t2.0 < t3.0);
        mgr.end_read(t1);
        mgr.commit_write(t2, HashSet::new());
        mgr.end_read(t3);
    }

    #[test]
    fn uncommitted_write_is_not_visible() {
        let mgr = TransactionManager::new();
        let writer = mgr.begin_write();
        let reader = mgr.begin_read();
        assert!(!mgr.is_visible(writer, reader));
        mgr.commit_write(writer, HashSet::new());
        assert!(mgr.is_visible(writer, reader));
        mgr.end_read(reader);
    }

    #[test]
    fn commit_write_is_visible_to_newer_readers() {
        let mgr = TransactionManager::new();
        let writer = mgr.begin_write();
        mgr.commit_write(writer, HashSet::new());
        let reader = mgr.begin_read();
        assert!(mgr.is_visible(writer, reader));
        mgr.end_read(reader);
    }

    #[test]
    fn committed_write_not_visible_to_older_readers() {
        let mgr = TransactionManager::new();
        let reader = mgr.begin_read();
        let writer = mgr.begin_write();
        mgr.commit_write(writer, HashSet::new());
        assert!(!mgr.is_visible(writer, reader));
        mgr.end_read(reader);
    }

    #[test]
    fn abort_removes_writer() {
        let mgr = TransactionManager::new();
        let writer = mgr.begin_write();
        mgr.abort(writer);
        assert!(!mgr.is_visible(writer, mgr.begin_read()));
    }

    #[test]
    fn has_committed_after_detects_newer_commits() {
        let mgr = TransactionManager::new();
        let tx1 = mgr.begin_read();
        assert!(!mgr.has_committed_after(tx1));
        let tx2 = mgr.begin_write();
        mgr.commit_write(tx2, HashSet::new());
        assert!(mgr.has_committed_after(tx1));
        mgr.end_read(tx1);
    }

    #[test]
    fn conflict_detection_catches_overlapping_writes() {
        let mgr = TransactionManager::new();
        let reader = mgr.begin_write(); // tx1 takes a snapshot

        let writer = mgr.begin_write(); // tx2 writes to page 5
        let mut pages = HashSet::new();
        pages.insert(5);
        mgr.commit_write(writer, pages);

        // tx1 read page 5 -- should detect conflict
        let mut read_set = HashSet::new();
        read_set.insert(5);
        assert!(mgr.has_conflicting_writes(reader, &read_set));

        // tx1 only read page 10 -- no conflict
        let mut read_set2 = HashSet::new();
        read_set2.insert(10);
        assert!(!mgr.has_conflicting_writes(reader, &read_set2));
    }

    #[test]
    fn gc_prunes_old_write_sets() {
        let mgr = TransactionManager::new();
        let w1 = mgr.begin_write();
        let mut pages = HashSet::new();
        pages.insert(1);
        mgr.commit_write(w1, pages);

        let _reader = mgr.begin_read(); // active reader at tx 3

        let w2 = mgr.begin_write();
        let mut pages2 = HashSet::new();
        pages2.insert(2);
        mgr.commit_write(w2, pages2);

        mgr.gc_write_sets();

        // w1 (tx 1) is older than the active reader (tx 3), should be pruned
        let sets = mgr.committed_write_sets.read().unwrap();
        assert!(!sets.contains_key(&w1));
        // w2 (tx 4) is newer, should still be present
        assert!(sets.contains_key(&w2));
    }
}
