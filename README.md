# ternary-mirror

State mirroring for GPU cluster replication with **ternary consistency states**: **{+1 = consistent, 0 = lagging, −1 = diverged}**. Implements primary-replica sync tracking, FNV-1a checksum-based divergence detection, and force-repair operations for maintaining state coherence across distributed GPU nodes.

## Why It Matters

GPU clusters running ternary inference need replicated state for fault tolerance and load balancing. Classical replication systems (Raft, Paxos) use binary consistency: either consistent or inconsistent. The ternary model adds a crucial intermediate state:

| State | Meaning | Action needed |
|-------|---------|---------------|
| +1 (Consistent) | Replica matches primary exactly | None |
| 0 (Lagging) | Replica is behind but not corrupted | Sync will fix it |
| −1 (Diverged) | Replica has conflicting data | Force repair required |

This three-state distinction prevents **unnecessary repairs**: a lagging replica can catch up with an incremental sync, while a diverged replica requires a full reset. Binary systems can't distinguish these cases, leading to either excessive repairs (performance loss) or undetected corruption (correctness loss).

## How It Works

### Replication Model

The system uses a **primary-replica** architecture:

```
Primary ───── write(k, v) ──► version++
           └── sync ───────► Replicas catch up
```

Each key-value pair on a node is stored as:

```
MirrorEntry = {
    key: Vec<u8>,
    version: u64,       // monotonic version number
    checksum: u64,      // FNV-1a hash of value
}
```

### Consistency Classification

Given primary entry P and replica entry R:

```
Consistency = {
    Consistent (+1)   if R exists ∧ R.checksum == P.checksum ∧ R.version == P.version
    Lagging (0)       if R missing ∧ sync_lag ≤ 5
                     ∨ R exists ∧ R.version < P.version ∧ R.checksum == P.checksum
    Diverged (−1)     if R missing ∧ sync_lag > 5
                     ∨ R exists ∧ R.checksum ≠ P.checksum
}
```

**Key insight**: checksum mismatch means data corruption (divergence), while version lag means the replica hasn't synced yet (lagging). These require different responses.

### Sync Protocol

Incremental sync copies only entries where the replica is behind:

```
sync_replica(r):
    for each entry in primary:
        if replica[entry.key] is None OR replica.version < entry.version:
            replica.put(entry.key, entry.version, entry.checksum)
            synced++
    replica.sync_lag = 0
```

### Checksum: FNV-1a

The crate uses Fowler-Noll-Vo 1a for checksum computation:

```
h = 14695981039346656037  (FNV offset basis, 64-bit)
for each byte b in data:
    h = h XOR b
    h = h × 1099511628211  (FNV prime)
return h
```

**Properties**:
- O(n) computation, O(1) space
- Avalanche: changing 1 bit changes ~50% of output bits
- Non-cryptographic: fast but not collision-resistant against adversaries

### Repair Protocol

When a replica diverges, force-repair overwrites all entries:

```
repair(r):
    for each entry in primary:
        replica.put(entry.key, entry.version, entry.checksum)
    repair_count++
```

This is O(N) where N = number of entries. The repair is conservative — it assumes all replica data may be corrupt.

### Complexity

| Operation | Time | Space |
|-----------|------|-------|
| `write_primary(key, data)` | O(|data|) | O(1) |
| `sync_replica(idx)` | O(E) | O(1) |
| `consistency(idx)` | O(E) | O(1) |
| `repair(idx)` | O(E) | O(E) |
| `write_primary` checksum | O(|data|) | O(1) |

Where E = number of entries, |data| = value size.

### Sync Lag Budget

The `sync_lag` counter tracks how far behind a replica has fallen. When it exceeds 5 (configurable threshold), the replica is classified as Diverged instead of Lagging. This prevents a replica from silently drifting indefinitely — if it hasn't synced after 5 missed updates, something is wrong.

## Quick Start

```rust
use ternary_mirror::{MirrorManager, Consistency};

let mut mm = MirrorManager::new("primary");
mm.add_replica("replica-1");
mm.add_replica("replica-2");

// Write to primary
mm.write_primary(b"model_weights".to_vec(), b"binary_data_here");
mm.write_primary(b"optimizer_state".to_vec(), b"more_data");

// Sync replica 1
let synced = mm.sync_replica(0);
println!("Synced {} entries to replica-1", synced);

// Check consistency
match mm.consistency(0) {
    Consistency::Consistent => println!("replica-1 is consistent"),
    Consistency::Lagging => println!("replica-1 is lagging behind"),
    Consistency::Diverged => println!("replica-1 has diverged!"),
}

// Replica 2 hasn't synced — should be lagging
assert_eq!(mm.consistency(1), Consistency::Lagging);

// Force repair if needed
let repaired = mm.repair(1);
println!("Repaired {} entries on replica-2", repaired);
```

## API

### `MirrorManager`

| Method | Description |
|--------|-------------|
| `new(primary_id)` | Create manager with primary node |
| `add_replica(id)` | Register a replica node |
| `write_primary(key, data) -> u64` | Write to primary, returns new version |
| `sync_replica(idx) -> usize` | Incremental sync, returns entry count |
| `consistency(idx) -> Consistency` | Classify replica consistency |
| `repair(idx) -> usize` | Force-copy all primary entries to replica |
| `replica_count() / repairs()` | Statistics |

### `Consistency`

| Variant | Value | Meaning |
|---------|-------|---------|
| `Consistent` | +1 | Exact match with primary |
| `Lagging` | 0 | Behind but recoverable via sync |
| `Diverged` | −1 | Corrupt, needs force repair |

## Architecture Notes

This crate implements the **γ (gamma) replication layer** of the γ + η = C framework:

- **γ (gamma)**: State consistency management — ensuring replicas agree on cluster state. This crate provides γ-level mirror tracking with ternary consistency classification.
- **η (eta)**: The compute workloads being replicated — model inference, tensor operations, and data pipelines running on the primary and replica nodes.
- **C**: The complete fault-tolerant GPU cluster system. γ ensures η-layer replicas are consistent, lagging, or diverged — the ternary classification drives automated recovery decisions.

The ternary consistency states {+1, 0, −1} parallel the ternary lease states and ternary marks used across the ecosystem, creating a unified three-state vocabulary for all coordination decisions.

## References

- **Primary-Backup Replication**: Budhiraja, N. et al., "Primary-Backup Protocols: Lower Bounds and Optimal Implementations," Fault-Tolerant Distributed Computing, 1993.
- **FNV-1a Hash**: Fowler, G., Noll, L.C. & Vo, P., "Fowler/Noll/Vo Hash," IETF Draft, 2012.
- **Eventual Consistency**: Vogels, W., "Eventually Consistent," Communications of the ACM, 52(1), 40-44, 2009.
- **Checksum-Based Detection**: Stonebraker, M., "The Case for Shared Nothing," IEEE Database Engineering Bulletin, 9(1), 1986.
- **CRDTs for Replication**: Shapiro, M. et al., "A Comprehensive Study of Convergent and Commutative Replicated Data Types," INRIA RR-7506, 2011.

## License

MIT
