# ternary-mirror

State mirroring for GPU cluster replication with ternary consistency tracking.

## Why This Exists

When you replicate GPU state across a cluster, you need to know: is a replica consistent with the primary, lagging behind, or diverged (corrupted)? Binary consistency (consistent/inconsistent) conflates "behind but correct" with "actively wrong." Ternary consistency separates these: `Consistent` means checksum match and same version count. `Lagging` means fewer entries but everything present is correct. `Diverged` means a checksum mismatch — data corruption.

This distinction drives different repair strategies: lagging replicas just need a sync, diverged replicas need investigation.

## Architecture

### Core Types

- **`Consistency`** — Ternary enum: `Consistent (+1)`, `Lagging (0)`, `Diverged (-1)`.
- **`MirrorEntry`** — A key-value entry with `key`, `version`, and `checksum`.
- **`MirrorNode`** — A node (primary or replica) holding entries in a HashMap.
- **`MirrorManager`** — Orchestrates one primary and N replicas with sync and repair.

### Consistency Logic

- **Consistent**: Replica has the same entry count as primary AND checksums match.
- **Lagging**: Replica has fewer entries but all present entries match checksums.
- **Diverged**: At least one entry has a different checksum than the primary.

## Usage

```rust
use ternary_mirror::{MirrorManager, Consistency};

let mut mm = MirrorManager::new("primary");
mm.add_replica("replica-1");
mm.add_replica("replica-2");

// Write to primary
mm.write_primary(b"weights_layer_0".to_vec(), &[1, 0, -1, 1]);

// Sync replica 0 — now consistent
mm.sync_replica(0);
assert_eq!(mm.consistency(0), Consistency::Consistent);

// Replica 1 is still lagging
assert_eq!(mm.consistency(1), Consistency::Lagging);

// Repair all diverged replicas
let repairs = mm.repair_all();
```

## API Reference

| Method | Returns | Description |
|--------|---------|-------------|
| `new(primary_id)` | `MirrorManager` | Create manager with a primary node |
| `add_replica(id)` | `()` | Add a replica node |
| `write_primary(key, data)` | `u64` | Write to primary, returns version |
| `sync_replica(idx)` | `usize` | Copy missing/newer entries, returns count synced |
| `consistency(idx)` | `Consistency` | Check ternary consistency of a replica |
| `repair_all()` | `Vec<String>` | Re-sync all diverged replicas |
| `replica_count()` | `usize` | Number of replicas |
| `repair_count()` | `u64` | Total repairs performed |

## The Deeper Idea

This mirrors the CAP theorem's consistency spectrum mapped to three actionable states. Rather than a binary "replica is healthy or not," you get a graduated signal: consistent replicas can serve reads, lagging replicas can serve stale reads with a warning, and diverged replicas must be taken out of rotation. This three-tier classification maps directly to read routing policies without needing external monitoring.

## Related Crates

- **ternary-reassembly** — fragment reassembly with ternary completion status
- **ternary-version** — version vectors with ternary comparison
- **ternary-resilience** — network resilience with ternary edge weights
