# Ternary Mirror — State Mirroring for GPU Cluster Replication

**Ternary Mirror** implements state replication across GPU cluster nodes with ternary consistency states: **+1 (Consistent)** — replica matches primary, **0 (Lagging)** — replica is behind but catchable, and **-1 (Diverged)** — replica has irreconcilable state. It provides write propagation, sync tracking, divergence detection, and automatic repair.

## Why It Matters

GPU clusters require state replication for fault tolerance and load balancing. Binary consistency models (consistent/inconsistent) lack the important middle ground: a replica that's one version behind is very different from one that's been partitioned for hours. The ternary model distinguishes these: lagging replicas can catch up with a delta sync, while divergent replicas require full repair. This enables smarter repair scheduling — fix lagging replicas immediately, schedule divergent repairs during low-load periods. For ternary inference fleets where model weights are {-1, 0, +1}, the mirror layer ensures all GPUs serve identical models.

## How It Works

### Mirror Entries

Each replicated key has a `MirrorEntry` with `version` and `checksum`. The primary node is the source of truth; replicas maintain their own entry maps.

### Write Propagation

When the primary writes a key:
1. Primary updates its entry with new version + checksum
2. Replicas are not synchronously updated (async replication)
3. On next sync cycle, replicas pull missing/updated entries

Write to primary: O(1). Sync cycle: O(k) for k changed entries.

### Consistency Classification

```
Consistent: entry counts match AND all checksums match
Lagging:    entry counts differ but sync_lag ≤ threshold (5)
Diverged:   entry counts differ AND sync_lag > threshold
```

A replica is Consistent if it has the same entries with the same versions. It's Lagging if it's missing entries but the gap is small (recoverable). It's Diverged if the gap exceeds the threshold (likely needs full repair).

### Sync Operation

`sync_replica(idx)` copies entries from primary that are newer than the replica's version. Returns count of synced entries. O(k) for k changed keys.

### Divergence Detection

Divergence is detected when:
1. Replica version is behind primary AND sync_lag exceeds threshold
2. Checksum mismatch on same-version entries (data corruption)

Divergent replicas are marked for repair — a full state copy from primary.

### Repair

Full repair copies all primary entries to the replica. O(n) for n entries. The `repair_count` tracks how many repairs have been performed.

## Quick Start

```rust
use ternary_mirror::{MirrorManager, Consistency};

let mut mgr = MirrorManager::new("primary");
mgr.add_replica("replica-1");
mgr.add_replica("replica-2");

// Write to primary
mgr.write_primary(b"model_weights".to_vec(), &[1, -1, 0, 1]);

// Sync replica 1
let synced = mgr.sync_replica(0);
let status = mgr.consistency(0);
// status == Consistency::Consistent after sync
```

```bash
cargo add ternary-mirror
```

## API

| Type / Function | Description |
|---|---|
| `Consistency` | `Consistent(1)`, `Lagging(0)`, `Diverged(-1)` |
| `MirrorManager` | `new(primary_id)`, `add_replica()`, `write_primary()`, `sync_replica()`, `consistency()` |
| `MirrorEntry` | `{ key, version, checksum }` |

## Architecture Notes

Mirror provides state replication for **SuperInstance** GPU fleets. The ternary consistency model maps to γ + η = C: consistent replicas contribute γ (reliable compute capacity), lagging replicas contribute η (entropy that resolves over time), and divergent replicas are η exceeding C (system breakdown). See [Architecture](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md).

## References

- Terry, D. et al. "Managing Update Conflicts in Bayou," *SOSP*, 1995 — eventual consistency.
- Lakshman, Avinash & Malik, Prashant. "Cassandra," *SIGMOD*, 2010 — distributed replication.
| DeCandia, Giuseppe et al. "Dynamo," *SOSP*, 2007 — consistent hashing and replication.

## License

Apache-2.0
