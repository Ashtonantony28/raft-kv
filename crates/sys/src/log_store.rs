use std::io::{Error, ErrorKind};

use raft::{Entry, HardState, LogIndex, Output, Restored, Term};

use crate::codec::{from_bytes, to_bytes};
use crate::crc32c::crc32c;
use crate::{Disk, FileId, HARD_STATE_SLOTS, LOG_FILE, Result};

/// Durable Raft log and hard state over a byte-level `Disk` (D-06, D-07, D-21).
///
/// Log file: records `len: u32 | crc32c(len, payload): u32 | Entry`, appended in index order.
/// Hard-state slots: `seq: u64 | len: u32 | crc32c(seq, len, payload): u32 | HardState`,
/// written alternately; recovery keeps the valid slot with the highest `seq`.
pub struct LogStore<D: Disk> {
    disk: D,
    entries: Vec<(u64, Term)>,
    end: u64,
    log_dirty: bool,
    hs_seq: u64,
    hs_slot: usize,
    hs_dirty: bool,
}

impl<D: Disk> LogStore<D> {
    pub fn open(mut disk: D) -> Result<(Self, Restored)> {
        let (hard_state, hs_seq, hs_slot) = recover_hard_state(&disk)?;
        let buf = read_all(&disk, LOG_FILE)?;
        let mut log: Vec<Entry> = Vec::new();
        let mut entries = Vec::new();
        let mut pos = 0;
        while let Some((payload, next)) = unframe(&buf[pos..], 0) {
            let Ok(entry) = from_bytes::<Entry>(payload) else {
                break;
            };
            let prev_term = log.last().map_or(0, |e| e.term);
            if entry.index != log.len() as u64 + 1 || entry.term < prev_term {
                break;
            }
            entries.push((pos as u64, entry.term));
            log.push(entry);
            pos += next;
        }
        let end = pos as u64;
        if end != buf.len() as u64 {
            disk.truncate(LOG_FILE, end)?;
            disk.sync(LOG_FILE)?;
        }
        let store = LogStore {
            disk,
            entries,
            end,
            log_dirty: false,
            hs_seq,
            hs_slot,
            hs_dirty: false,
        };
        Ok((store, Restored { hard_state, log }))
    }

    pub fn apply(&mut self, out: &Output) -> Result<()> {
        match out {
            Output::PersistHardState(hs) => self.write_hard_state(hs),
            Output::Truncate { from } => self.truncate(*from),
            Output::Append(entries) => self.append(entries),
            Output::Send(_) | Output::Apply(_) | Output::Proposed { .. } => Ok(()),
        }
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.hs_dirty {
            self.disk.sync(HARD_STATE_SLOTS[self.hs_slot])?;
            self.hs_seq += 1;
            self.hs_slot ^= 1;
            self.hs_dirty = false;
        }
        if self.log_dirty {
            self.disk.sync(LOG_FILE)?;
            self.log_dirty = false;
        }
        Ok(())
    }

    pub fn disk(&self) -> &D {
        &self.disk
    }

    pub fn into_disk(self) -> D {
        self.disk
    }

    fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }

    fn write_hard_state(&mut self, hs: &HardState) -> Result<()> {
        let mut buf = Vec::new();
        frame(&(self.hs_seq + 1).to_le_bytes(), &to_bytes(hs), &mut buf);
        self.disk.write(HARD_STATE_SLOTS[self.hs_slot], 0, &buf)?;
        self.hs_dirty = true;
        Ok(())
    }

    fn truncate(&mut self, from: LogIndex) -> Result<()> {
        if from == 0 || from > self.last_index() + 1 {
            return Err(invalid(format!(
                "truncate from {from} with last index {}",
                self.last_index()
            )));
        }
        if from == self.last_index() + 1 {
            return Ok(());
        }
        let keep = (from - 1) as usize;
        self.end = self.entries[keep].0;
        self.entries.truncate(keep);
        self.disk.truncate(LOG_FILE, self.end)?;
        self.disk.sync(LOG_FILE)?;
        self.log_dirty = false;
        Ok(())
    }

    fn append(&mut self, new: &[Entry]) -> Result<()> {
        let mut buf = Vec::new();
        let mut index = self.last_index();
        let mut term = self.entries.last().map_or(0, |&(_, t)| t);
        let mut pending = Vec::with_capacity(new.len());
        for e in new {
            if e.index != index + 1 || e.term < term {
                return Err(invalid(format!(
                    "append ({}, {}) after ({index}, {term})",
                    e.index, e.term
                )));
            }
            (index, term) = (e.index, e.term);
            pending.push((self.end + buf.len() as u64, e.term));
            frame(&[], &to_bytes(e), &mut buf);
        }
        if buf.is_empty() {
            return Ok(());
        }
        self.disk.write(LOG_FILE, self.end, &buf)?;
        self.end += buf.len() as u64;
        self.entries.extend(pending);
        self.log_dirty = true;
        Ok(())
    }
}

fn recover_hard_state<D: Disk>(disk: &D) -> Result<(HardState, u64, usize)> {
    let mut best: Option<(u64, usize, HardState)> = None;
    for (slot, &file) in HARD_STATE_SLOTS.iter().enumerate() {
        let buf = read_all(disk, file)?;
        let Some((payload, _)) = unframe(&buf, 8) else {
            continue;
        };
        let Ok(hs) = from_bytes::<HardState>(payload) else {
            continue;
        };
        let seq = u64::from_le_bytes(buf[..8].try_into().expect("unframe checked length"));
        if best.is_none_or(|(s, _, _)| seq > s) {
            best = Some((seq, slot, hs));
        }
    }
    Ok(match best {
        Some((seq, slot, hs)) => (hs, seq, slot ^ 1),
        None => (HardState::default(), 0, 0),
    })
}

fn read_all<D: Disk>(disk: &D, f: FileId) -> Result<Vec<u8>> {
    let len = usize::try_from(disk.len(f)?).map_err(|_| invalid("file too large".into()))?;
    let mut buf = vec![0; len];
    disk.read(f, 0, &mut buf)?;
    Ok(buf)
}

/// Appends `prefix | len: u32 | crc: u32 | payload`, where crc covers prefix, len and payload.
fn frame(prefix: &[u8], payload: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(payload.len())
        .expect("record over 4 GiB")
        .to_le_bytes();
    out.extend_from_slice(prefix);
    out.extend_from_slice(&len);
    out.extend_from_slice(&crc32c(&[prefix, &len, payload]).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Parses a record written by `frame` with a `prefix_len`-byte prefix at the start of `buf`.
/// Returns the payload and the record's total length, or `None` if it is short or corrupt.
fn unframe(buf: &[u8], prefix_len: usize) -> Option<(&[u8], usize)> {
    let header = buf.get(..prefix_len + 8)?;
    let (prefix, rest) = header.split_at(prefix_len);
    let (len, crc) = rest.split_at(4);
    let total = prefix_len + 8 + u32::from_le_bytes(len.try_into().ok()?) as usize;
    let payload = buf.get(prefix_len + 8..total)?;
    (crc32c(&[prefix, len, payload]).to_le_bytes() == crc).then_some((payload, total))
}

fn invalid(msg: String) -> Error {
    Error::new(ErrorKind::InvalidInput, msg)
}
