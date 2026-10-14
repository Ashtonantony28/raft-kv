use std::fmt::Debug;
use std::io::{self, Write};

use raft::NodeId;
use sys::Nanos;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fnv64(u64);

impl Default for Fnv64 {
    fn default() -> Self {
        Fnv64(FNV_OFFSET)
    }
}

impl Fnv64 {
    pub fn update(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(FNV_PRIME);
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}

/// Writes trace lines to an optional sink while hashing every byte.
pub struct Tracer<'a> {
    sink: Option<&'a mut dyn Write>,
    hash: Fnv64,
    seq: u64,
    line: Vec<u8>,
}

impl<'a> Tracer<'a> {
    pub fn new(sink: Option<&'a mut dyn Write>) -> Self {
        Tracer {
            sink,
            hash: Fnv64::default(),
            seq: 0,
            line: Vec::new(),
        }
    }

    pub fn record(
        &mut self,
        time: Nanos,
        node: Option<NodeId>,
        kind: &str,
        payload: &impl Debug,
    ) -> io::Result<()> {
        self.line.clear();
        match node {
            Some(n) => write!(self.line, "{time} {} {n} {kind} {payload:?}", self.seq)?,
            None => write!(self.line, "{time} {} - {kind} {payload:?}", self.seq)?,
        }
        self.line.push(b'\n');
        self.seq += 1;
        self.hash.update(&self.line);
        if let Some(w) = self.sink.as_mut() {
            w.write_all(&self.line)?;
        }
        Ok(())
    }

    pub fn hash(&self) -> u64 {
        self.hash.finish()
    }

    pub fn events(&self) -> u64 {
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a64_reference_vectors() {
        let h = |s: &[u8]| {
            let mut f = Fnv64::default();
            f.update(s);
            f.finish()
        };
        assert_eq!(h(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(h(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(h(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn golden_trace_lines() {
        let mut out = Vec::new();
        let mut t = Tracer::new(Some(&mut out));
        t.record(1_000, Some(2), "tick", &()).unwrap();
        t.record(2_500, None, "partition", &[[1u64, 2], [3, 0]])
            .unwrap();
        let hash = t.hash();
        assert_eq!(
            String::from_utf8(out.clone()).unwrap(),
            "1000 0 2 tick ()\n2500 1 - partition [[1, 2], [3, 0]]\n"
        );
        let mut f = Fnv64::default();
        f.update(&out);
        assert_eq!(hash, f.finish());
    }

    #[test]
    fn hash_without_sink_matches() {
        let mut a = Tracer::new(None);
        let mut buf = Vec::new();
        let mut b = Tracer::new(Some(&mut buf));
        for t in [(1, "x"), (2, "y")] {
            a.record(t.0, Some(1), t.1, &t).unwrap();
            b.record(t.0, Some(1), t.1, &t).unwrap();
        }
        assert_eq!(a.hash(), b.hash());
    }
}
