//! SG2-H end-to-end: five nodes under random message loss, duplication, reordering and crashes.
//! The harness checks election safety, log matching, leader completeness and state machine
//! safety after every step; afterwards the healed cluster must elect a leader and catch up.

mod common;

use common::*;
use raft::{Input, Role};

struct XorShift(u64);

impl XorShift {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn leader(c: &Cluster) -> Option<u64> {
    (1..=5).find(|&id| c.role(id) == Role::Leader)
}

#[test]
#[ignore = "SG2-H"]
fn random_faults_preserve_safety() {
    for seed in 1..=30 {
        let mut rng = XorShift(seed * 0x9E37_79B9_7F4A_7C15);
        let mut c = Cluster::fresh(5);
        let mut tag = 0;
        for _ in 0..5_000 {
            match rng.below(100) {
                0..45 => {
                    c.tick(1 + rng.below(5));
                }
                45..85 if !c.net.is_empty() => {
                    let i = rng.below(c.net.len() as u64) as usize;
                    c.deliver_nth(i);
                }
                85..90 if !c.net.is_empty() => {
                    let i = rng.below(c.net.len() as u64) as usize;
                    c.net.remove(i);
                }
                90..93 if !c.net.is_empty() => {
                    let i = rng.below(c.net.len() as u64) as usize;
                    let dup = c.net[i].clone();
                    c.net.push_back(dup);
                }
                93..99 => {
                    if let Some(id) = leader(&c) {
                        tag += 1;
                        c.step(
                            id,
                            Input::Propose {
                                tag,
                                data: tag.to_le_bytes().to_vec(),
                            },
                        );
                    }
                }
                99 => c.restart(1 + rng.below(5), rng.below(u64::MAX)),
                _ => {}
            }
        }

        let mut rounds = 0;
        loop {
            if let Some(id) = leader(&c) {
                let s = c.raft(id).status();
                if s.commit == s.last_index
                    && (1..=5).all(|n| c.raft(n).status().commit == s.commit)
                {
                    break;
                }
            }
            rounds += 1;
            assert!(rounds < 2_000, "seed {seed}: cluster did not recover");
            for id in 1..=5 {
                c.tick(id);
            }
            c.deliver_all();
        }
    }
}
