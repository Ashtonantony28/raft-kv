# raft-kv

A Raft-replicated key-value store in Rust, tested by deterministic simulation: one seed drives time, network, disk, and crashes, and any run replays exactly.

```
cargo test --workspace
```

Plan and design decisions: `PLAN.md`, `DECISIONS.md` (added at SG9).
