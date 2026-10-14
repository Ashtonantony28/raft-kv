pub mod codec;
mod crc32c;
mod log_store;

use kv::{ClientId, ClientReply};
use raft::Envelope;

pub use log_store::LogStore;
pub use std::io::Result;

pub type Nanos = u64;
pub type FileId = u32;

pub const LOG_FILE: FileId = 0;
pub const HARD_STATE_SLOTS: [FileId; 2] = [1, 2];

pub trait Clock {
    fn now(&self) -> Nanos;
}

pub trait Rng {
    fn next_u64(&mut self) -> u64;
}

pub trait Transport {
    fn send(&mut self, env: Envelope);
    fn reply(&mut self, to: ClientId, reply: ClientReply);
}

/// Byte-addressed files (D-06). `sync(f)` makes every earlier `write` and `truncate` of `f`
/// durable; on crash, unsynced operations may each be lost and the last write torn (D-07).
/// Bytes never written (gaps from a lost write) read as zero.
pub trait Disk {
    fn len(&self, f: FileId) -> Result<u64>;
    fn read(&self, f: FileId, offset: u64, buf: &mut [u8]) -> Result<()>;
    fn write(&mut self, f: FileId, offset: u64, data: &[u8]) -> Result<()>;
    fn truncate(&mut self, f: FileId, len: u64) -> Result<()>;
    fn sync(&mut self, f: FileId) -> Result<()>;
}
