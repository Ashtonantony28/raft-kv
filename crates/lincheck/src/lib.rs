use std::io::{BufRead, Write};

use kv::{ClientId, Op, OpResult};
use serde::{Deserialize, Serialize};
use sys::Nanos;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub client: ClientId,
    pub op: Op,
    pub call: Nanos,
    pub ret: Option<(Nanos, OpResult)>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct History {
    pub ops: Vec<Operation>,
}

impl History {
    pub fn write_jsonl(&self, mut w: impl Write) -> std::io::Result<()> {
        for op in &self.ops {
            serde_json::to_writer(&mut w, op)?;
            w.write_all(b"\n")?;
        }
        Ok(())
    }

    pub fn read_jsonl(r: impl BufRead) -> std::io::Result<Self> {
        let mut ops = Vec::new();
        for line in r.lines() {
            let line = line?;
            if !line.is_empty() {
                ops.push(serde_json::from_str(&line)?);
            }
        }
        Ok(History { ops })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    Violation { key: Vec<u8>, witness: Vec<usize> },
    Unknown,
}

pub fn check(h: &History, max_steps: u64) -> Verdict {
    let _ = (h, max_steps);
    todo!("SG8")
}
