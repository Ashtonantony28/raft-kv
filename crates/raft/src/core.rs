use crate::{Config, Entry, HardState, Input, Output, Restored, Status};

#[derive(Clone, Debug)]
pub struct Raft {
    cfg: Config,
    hard_state: HardState,
    log: Vec<Entry>,
    entropy: u64,
}

impl Raft {
    pub fn new(cfg: Config, restored: Restored, entropy: u64) -> Self {
        Raft {
            cfg,
            hard_state: restored.hard_state,
            log: restored.log,
            entropy,
        }
    }

    pub fn step(&mut self, input: Input, out: &mut Vec<Output>) {
        let _ = (&self.cfg, self.hard_state, self.entropy, input, out);
        todo!("SG2")
    }

    pub fn status(&self) -> Status {
        todo!("SG2")
    }

    pub fn log(&self) -> &[Entry] {
        &self.log
    }
}
