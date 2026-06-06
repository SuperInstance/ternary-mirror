//! # ternary-mirror
//!
//! State mirroring for GPU cluster replication with ternary consistency.

use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency { Consistent = 1, Lagging = 0, Diverged = -1 }

#[derive(Debug, Clone)]
pub struct MirrorEntry {
    pub key: Vec<u8>,
    pub version: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone)]
pub struct MirrorNode {
    pub id: String,
    entries: HashMap<Vec<u8>, MirrorEntry>,
    sync_lag: u64,
}

impl MirrorNode {
    pub fn new(id: &str) -> Self { Self { id: id.into(), entries: HashMap::new(), sync_lag: 0 } }

    pub fn put(&mut self, key: Vec<u8>, version: u64, checksum: u64) {
        self.entries.insert(key.clone(), MirrorEntry { key, version, checksum });
    }

    pub fn get(&self, key: &[u8]) -> Option<&MirrorEntry> { self.entries.get(key) }

    pub fn entry_count(&self) -> usize { self.entries.len() }
}

pub struct MirrorManager {
    primary: MirrorNode,
    replicas: Vec<MirrorNode>,
    repair_count: u64,
}

impl MirrorManager {
    pub fn new(primary_id: &str) -> Self {
        Self { primary: MirrorNode::new(primary_id), replicas: Vec::new(), repair_count: 0 }
    }

    pub fn add_replica(&mut self, id: &str) { self.replicas.push(MirrorNode::new(id)); }

    pub fn write_primary(&mut self, key: Vec<u8>, data: &[u8]) -> u64 {
        let version = self.primary.entry_count() as u64 + 1;
        let checksum = simple_hash(data);
        self.primary.put(key, version, checksum);
        version
    }

    /// Sync a replica: copy entries from primary that are newer.
    pub fn sync_replica(&mut self, replica_idx: usize) -> usize {
        let mut synced = 0;
        let primary_entries: Vec<MirrorEntry> = self.primary.entries.values().cloned().collect();
        for entry in &primary_entries {
            let replica = &mut self.replicas[replica_idx];
            let needs_sync = match replica.get(&entry.key) {
                None => true,
                Some(r) => r.version < entry.version,
            };
            if needs_sync {
                replica.put(entry.key.clone(), entry.version, entry.checksum);
                synced += 1;
            }
        }
        if synced > 0 { self.replicas[replica_idx].sync_lag = 0; }
        synced
    }

    pub fn consistency(&self, replica_idx: usize) -> Consistency {
        let replica = &self.replicas[replica_idx];
        if replica.entry_count() != self.primary.entry_count() {
            if replica.sync_lag > 5 { return Consistency::Diverged; }
            return Consistency::Lagging;
        }
        for (key, primary_entry) in &self.primary.entries {
            match replica.get(key) {
                None => return Consistency::Diverged,
                Some(r) if r.checksum != primary_entry.checksum => return Consistency::Diverged,
                Some(r) if r.version < primary_entry.version => return Consistency::Lagging,
                _ => {}
            }
        }
        Consistency::Consistent
    }

    /// Repair a diverged replica by force-copying all primary entries.
    pub fn repair(&mut self, replica_idx: usize) -> usize {
        let primary_entries: Vec<MirrorEntry> = self.primary.entries.values().cloned().collect();
        let count = primary_entries.len();
        for entry in &primary_entries {
            self.replicas[replica_idx].put(entry.key.clone(), entry.version, entry.checksum);
        }
        self.repair_count += 1;
        count
    }

    pub fn replica_count(&self) -> usize { self.replicas.len() }
    pub fn repairs(&self) -> u64 { self.repair_count }
}

fn simple_hash(data: &[u8]) -> u64 {
    let mut h: u64 = 14695981039346656037;
    for &b in data { h ^= b as u64; h = h.wrapping_mul(1099511628211); }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read() {
        let mut mm = MirrorManager::new("primary");
        mm.write_primary(b"key1".to_vec(), b"data1");
        assert!(mm.primary.get(b"key1").is_some());
    }

    #[test]
    fn test_sync_replica() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.write_primary(b"k".to_vec(), b"v");
        let synced = mm.sync_replica(0);
        assert_eq!(synced, 1);
        assert_eq!(mm.replicas[0].entry_count(), 1);
    }

    #[test]
    fn test_consistency_after_sync() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.write_primary(b"k".to_vec(), b"v");
        mm.sync_replica(0);
        assert_eq!(mm.consistency(0), Consistency::Consistent);
    }

    #[test]
    fn test_lagging() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.write_primary(b"k".to_vec(), b"v");
        assert_eq!(mm.consistency(0), Consistency::Lagging);
    }

    #[test]
    fn test_repair() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.write_primary(b"k1".to_vec(), b"v1");
        mm.write_primary(b"k2".to_vec(), b"v2");
        mm.repair(0);
        assert_eq!(mm.consistency(0), Consistency::Consistent);
        assert_eq!(mm.repairs(), 1);
    }

    #[test]
    fn test_diverged_after_corruption() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.write_primary(b"k".to_vec(), b"v1");
        mm.sync_replica(0);
        // Corrupt replica
        mm.replicas[0].put(b"k".to_vec(), 1, 999);
        assert_eq!(mm.consistency(0), Consistency::Diverged);
    }

    #[test]
    fn test_multi_replica() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.add_replica("r2");
        mm.write_primary(b"k".to_vec(), b"v");
        mm.sync_replica(0);
        assert_eq!(mm.replica_count(), 2);
        assert_eq!(mm.consistency(0), Consistency::Consistent);
        assert_eq!(mm.consistency(1), Consistency::Lagging);
    }

    #[test]
    fn test_incremental_sync() {
        let mut mm = MirrorManager::new("primary");
        mm.add_replica("r1");
        mm.write_primary(b"k1".to_vec(), b"v1");
        mm.sync_replica(0);
        mm.write_primary(b"k2".to_vec(), b"v2");
        let synced = mm.sync_replica(0);
        assert_eq!(synced, 1); // only k2 synced
    }
}
