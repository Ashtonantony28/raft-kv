mod rng;
pub mod trace;

use std::io::Write;

use lincheck::History;
use raft::NodeId;
use sys::Nanos;

pub use rng::SimRng;

const MS: Nanos = 1_000_000;
const S: Nanos = 1_000 * MS;

#[derive(Clone, Debug, PartialEq)]
pub struct FaultConfig {
    pub net_drop: f64,
    pub net_dup: f64,
    pub net_delay: (Nanos, Nanos),
    pub partition_every: (Nanos, Nanos),
    pub partition_heal: (Nanos, Nanos),
    pub crash_every: (Nanos, Nanos),
    pub restart_after: (Nanos, Nanos),
    pub torn_write: f64,
    pub lose_unsynced: f64,
    pub lying_fsync: f64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        FaultConfig {
            net_drop: 0.02,
            net_dup: 0.01,
            net_delay: (MS, 50 * MS),
            partition_every: (2 * S, 10 * S),
            partition_heal: (S / 2, 5 * S),
            crash_every: (5 * S, 20 * S),
            restart_after: (100 * MS, 3 * S),
            torn_write: 0.3,
            lose_unsynced: 0.5,
            lying_fsync: 0.0,
        }
    }
}

impl FaultConfig {
    pub fn none() -> Self {
        FaultConfig {
            net_drop: 0.0,
            net_dup: 0.0,
            net_delay: (MS, MS),
            partition_every: (Nanos::MAX, Nanos::MAX),
            partition_heal: (0, 0),
            crash_every: (Nanos::MAX, Nanos::MAX),
            restart_after: (0, 0),
            torn_write: 0.0,
            lose_unsynced: 0.0,
            lying_fsync: 0.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: usize,
    pub clients: usize,
    pub tick: Nanos,
    pub max_time: Nanos,
    pub ops_per_client: usize,
    pub faults: FaultConfig,
}

impl SimConfig {
    pub fn new(seed: u64) -> Self {
        SimConfig {
            seed,
            nodes: 3,
            clients: 3,
            tick: 10 * MS,
            max_time: 60 * S,
            ops_per_client: 100,
            faults: FaultConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    Oracle {
        invariant: &'static str,
        detail: String,
    },
    NotLinearizable {
        key: Vec<u8>,
        witness: Vec<usize>,
    },
    CheckerBudgetExhausted,
    NodePanic {
        node: NodeId,
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct SimOutcome {
    pub seed: u64,
    pub trace_hash: u64,
    pub violation: Option<Violation>,
    pub history: History,
    pub sim_time: Nanos,
    pub events: u64,
}

pub fn run(cfg: &SimConfig, trace: Option<&mut dyn Write>) -> SimOutcome {
    let _ = (cfg, trace);
    todo!("SG5/SG7")
}
