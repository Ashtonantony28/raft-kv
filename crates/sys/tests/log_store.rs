use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::rc::Rc;

use proptest::prelude::*;
use raft::{Entry, HardState, Output, Payload, Restored};
use sys::{Disk, FileId, HARD_STATE_SLOTS, LOG_FILE, LogStore, Result};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

#[derive(Clone, Debug)]
enum DiskOp {
    Write(u64, Vec<u8>),
    Truncate(u64),
}

/// In-memory disk following D-07: synced data survives a crash; each unsynced operation
/// is independently lost, and the last unsynced write of a file may be torn.
/// With `budget` set, every mutating call fails once that many have succeeded.
#[derive(Clone, Debug, Default)]
struct CrashDisk {
    durable: BTreeMap<FileId, Vec<u8>>,
    current: BTreeMap<FileId, Vec<u8>>,
    pending: BTreeMap<FileId, Vec<DiskOp>>,
    budget: Rc<Cell<Option<u64>>>,
}

fn apply_op(file: &mut Vec<u8>, op: &DiskOp) {
    match op {
        DiskOp::Write(off, data) => {
            let off = *off as usize;
            if file.len() < off + data.len() {
                file.resize(off + data.len(), 0);
            }
            file[off..off + data.len()].copy_from_slice(data);
        }
        DiskOp::Truncate(len) => file.resize(*len as usize, 0),
    }
}

impl CrashDisk {
    fn spend(&mut self) -> Result<()> {
        match self.budget.get() {
            Some(0) => Err(Error::other("crashed")),
            Some(n) => {
                self.budget.set(Some(n - 1));
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn mutate(&mut self, f: FileId, op: DiskOp) -> Result<()> {
        self.spend()?;
        apply_op(self.current.entry(f).or_default(), &op);
        self.pending.entry(f).or_default().push(op);
        Ok(())
    }

    fn crash(&self, rng: &mut Rng) -> CrashDisk {
        let mut files = self.durable.clone();
        for (&f, ops) in &self.pending {
            let file = files.entry(f).or_default();
            let last_write = ops.iter().rposition(|op| matches!(op, DiskOp::Write(..)));
            for (i, op) in ops.iter().enumerate() {
                if !rng.chance(50) {
                    continue;
                }
                match op {
                    DiskOp::Write(off, data) if Some(i) == last_write && rng.chance(50) => {
                        let keep = rng.below(data.len() as u64 + 1) as usize;
                        apply_op(file, &DiskOp::Write(*off, data[..keep].to_vec()));
                    }
                    op => apply_op(file, op),
                }
            }
        }
        CrashDisk {
            durable: files.clone(),
            current: files,
            ..CrashDisk::default()
        }
    }
}

impl Disk for CrashDisk {
    fn len(&self, f: FileId) -> Result<u64> {
        Ok(self.current.get(&f).map_or(0, |d| d.len() as u64))
    }

    fn read(&self, f: FileId, offset: u64, buf: &mut [u8]) -> Result<()> {
        let data = self.current.get(&f).map_or(&[][..], |d| d.as_slice());
        let src = data
            .get(offset as usize..offset as usize + buf.len())
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "read past end"))?;
        buf.copy_from_slice(src);
        Ok(())
    }

    fn write(&mut self, f: FileId, offset: u64, data: &[u8]) -> Result<()> {
        self.mutate(f, DiskOp::Write(offset, data.to_vec()))
    }

    fn truncate(&mut self, f: FileId, len: u64) -> Result<()> {
        self.mutate(f, DiskOp::Truncate(len))
    }

    fn sync(&mut self, f: FileId) -> Result<()> {
        self.spend()?;
        let data = self.current.get(&f).cloned().unwrap_or_default();
        self.durable.insert(f, data);
        self.pending.remove(&f);
        Ok(())
    }
}

fn entry(term: u64, index: u64) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Command(format!("{term}/{index}").into_bytes()),
    }
}

fn entries(terms: &[u64], first: u64) -> Vec<Entry> {
    terms
        .iter()
        .zip(first..)
        .map(|(&t, i)| entry(t, i))
        .collect()
}

fn hs(term: u64, voted_for: Option<u64>) -> HardState {
    HardState { term, voted_for }
}

fn open(disk: CrashDisk) -> (LogStore<CrashDisk>, Restored) {
    LogStore::open(disk).expect("open")
}

fn reopen(store: LogStore<CrashDisk>) -> (LogStore<CrashDisk>, Restored) {
    open(store.into_disk())
}

fn write(store: &mut LogStore<CrashDisk>, outs: &[Output]) {
    for out in outs {
        store.apply(out).expect("apply");
    }
    store.sync().expect("sync");
}

fn corrupt(disk: &mut CrashDisk, f: FileId, at: usize) {
    for map in [&mut disk.current, &mut disk.durable] {
        map.get_mut(&f).unwrap()[at] ^= 0x40;
    }
}

#[test]
fn empty_disk_restores_nothing() {
    let (_, r) = open(CrashDisk::default());
    assert_eq!(r.hard_state, HardState::default());
    assert!(r.log.is_empty());
}

#[test]
fn append_sync_reopen() {
    let (mut s, _) = open(CrashDisk::default());
    write(
        &mut s,
        &[
            Output::PersistHardState(hs(2, Some(3))),
            Output::Append(entries(&[1, 1, 2], 1)),
            Output::Append(vec![]),
            Output::Append(entries(&[2], 4)),
        ],
    );
    let (_, r) = reopen(s);
    assert_eq!(r.hard_state, hs(2, Some(3)));
    assert_eq!(r.log, entries(&[1, 1, 2, 2], 1));
}

#[test]
fn truncate_then_append() {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::Append(entries(&[1, 1, 1, 1], 1))]);
    write(
        &mut s,
        &[
            Output::Truncate { from: 5 },
            Output::Truncate { from: 3 },
            Output::Append(entries(&[3], 3)),
        ],
    );
    let (mut s, r) = reopen(s);
    assert_eq!(r.log, entries(&[1, 1, 3], 1));
    write(&mut s, &[Output::Truncate { from: 1 }]);
    let (_, r) = reopen(s);
    assert!(r.log.is_empty());
}

#[test]
fn truncate_is_durable_without_explicit_sync() {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::Append(entries(&[1, 1, 1], 1))]);
    s.apply(&Output::Truncate { from: 2 }).unwrap();
    s.apply(&Output::Append(entries(&[2], 2))).unwrap();
    for seed in 0..64 {
        let (_, r) = open(s.disk().crash(&mut Rng(seed)));
        assert!(r.log == entries(&[1], 1) || r.log == entries(&[1, 2], 1));
    }
}

#[test]
fn ignores_non_storage_outputs() {
    let (mut s, _) = open(CrashDisk::default());
    write(
        &mut s,
        &[
            Output::Apply(entries(&[1], 1)),
            Output::Proposed {
                tag: 1,
                result: Ok(1),
            },
        ],
    );
    assert!(s.disk().current.is_empty());
}

#[test]
fn rejects_invalid_outputs() {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::Append(entries(&[2, 2], 1))]);
    for bad in [
        Output::Append(entries(&[2], 4)),
        Output::Append(entries(&[2], 2)),
        Output::Append(entries(&[1], 3)),
        Output::Append(vec![entry(2, 3), entry(2, 5)]),
        Output::Truncate { from: 0 },
        Output::Truncate { from: 4 },
    ] {
        let err = s.apply(&bad).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
    let (_, r) = reopen(s);
    assert_eq!(r.log, entries(&[2, 2], 1));
}

#[test]
fn corrupt_record_truncates_log_from_there() {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::Append(entries(&[1, 1, 1], 1))]);
    let mut disk = s.into_disk();
    let len = disk.len(LOG_FILE).unwrap() as usize;
    corrupt(&mut disk, LOG_FILE, len / 2);
    let (mut s, r) = open(disk);
    assert_eq!(r.log, entries(&[1], 1));
    write(&mut s, &[Output::Append(entries(&[4], 2))]);
    let (_, r) = reopen(s);
    assert_eq!(r.log, entries(&[1, 4], 1));
}

#[test]
fn every_torn_tail_recovers_a_prefix() {
    let (mut s, _) = open(CrashDisk::default());
    let log = entries(&[1, 2, 2, 3], 1);
    write(&mut s, &[Output::Append(log.clone())]);
    let full = s.disk().current[&LOG_FILE].clone();
    for cut in 0..=full.len() {
        let mut disk = CrashDisk::default();
        disk.current.insert(LOG_FILE, full[..cut].to_vec());
        let (s, r) = open(disk);
        assert!(log.starts_with(&r.log), "cut {cut}");
        assert_eq!(r.log.len() == log.len(), cut == full.len());
        let (_, again) = reopen(s);
        assert_eq!(again.log, r.log);
    }
}

fn log_bytes(terms: &[u64]) -> Vec<u8> {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::Append(entries(terms, 1))]);
    s.disk().current[&LOG_FILE].clone()
}

#[test]
fn checksummed_records_out_of_sequence_are_dropped() {
    let valid = log_bytes(&[2, 2]);
    let ones = log_bytes(&[1, 1, 1]);
    let first = log_bytes(&[1]).len();
    let third = &ones[log_bytes(&[1, 1]).len()..];
    for stale in [&ones[..first], third] {
        let mut disk = CrashDisk::default();
        disk.current.insert(LOG_FILE, [&valid[..], stale].concat());
        let (s, r) = open(disk);
        assert_eq!(r.log, entries(&[2, 2], 1));
        assert_eq!(s.disk().len(LOG_FILE).unwrap(), valid.len() as u64);
    }
}

#[test]
fn hard_state_falls_back_to_older_slot() {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::PersistHardState(hs(1, None))]);
    write(&mut s, &[Output::PersistHardState(hs(2, Some(1)))]);
    let (s, r) = reopen(s);
    assert_eq!(r.hard_state, hs(2, Some(1)));
    let mut disk = s.into_disk();
    corrupt(&mut disk, HARD_STATE_SLOTS[1], 3);
    let (_, r) = open(disk);
    assert_eq!(r.hard_state, hs(1, None));
}

#[test]
fn unsynced_hard_state_never_clobbers_synced_slot() {
    let (mut s, _) = open(CrashDisk::default());
    write(&mut s, &[Output::PersistHardState(hs(1, Some(1)))]);
    for t in 2..6 {
        s.apply(&Output::PersistHardState(hs(t, Some(t)))).unwrap();
    }
    for seed in 0..64 {
        let (_, r) = open(s.disk().crash(&mut Rng(seed)));
        assert!((1..6).contains(&r.hard_state.term), "{:?}", r.hard_state);
        assert_eq!(r.hard_state.voted_for, Some(r.hard_state.term));
    }
}

#[derive(Clone, Debug, Default)]
struct Model {
    log: Vec<Entry>,
    /// Length of the log prefix that was synced and has not been truncated since.
    floor: usize,
    hs: HardState,
    synced_hs: HardState,
    unsynced_hs: Vec<HardState>,
}

fn floor_after(floor: usize, out: &Output) -> usize {
    match out {
        Output::Truncate { from } => floor.min(*from as usize - 1),
        _ => floor,
    }
}

fn applied(log: &[Entry], out: &Output) -> Vec<Entry> {
    let mut log = log.to_vec();
    match out {
        Output::Truncate { from } => log.truncate(*from as usize - 1),
        Output::Append(es) => log.extend_from_slice(es),
        _ => {}
    }
    log
}

fn random_output(rng: &mut Rng, log: &[Entry]) -> Option<Output> {
    let last = log.last().map_or((0, 1), |e| (e.index, e.term));
    Some(match rng.below(100) {
        0..40 => {
            let mut term = last.1;
            let es = (1..=rng.below(4) + 1)
                .map(|i| {
                    term += rng.below(3) / 2;
                    let payload = match rng.below(3) {
                        0 => Payload::Noop,
                        n => Payload::Command(vec![n as u8; rng.below(20) as usize]),
                    };
                    Entry {
                        term,
                        index: last.0 + i,
                        payload,
                    }
                })
                .collect();
            Output::Append(es)
        }
        40..50 => Output::Truncate {
            from: rng.below(last.0 + 1) + 1,
        },
        50..70 => Output::PersistHardState(hs(rng.below(10), rng.chance(50).then(|| rng.below(5)))),
        70..75 => Output::Apply(log.to_vec()),
        _ => return None,
    })
}

/// Runs random outputs and syncs until the disk budget runs out or the round ends, then
/// crashes and checks the recovered state. Returns the disk after the crash.
fn round(rng: &mut Rng, disk: CrashDisk, model: &mut Model) -> CrashDisk {
    let (mut store, r) = open(disk);
    assert_eq!(r.log, model.log);
    assert_eq!(r.hard_state, model.hs);
    store
        .disk()
        .budget
        .set(rng.chance(70).then(|| rng.below(60)));

    let mut inflight = None;
    for _ in 0..rng.below(50) {
        match random_output(rng, &model.log) {
            Some(out) => {
                let res = store.apply(&out);
                if let Output::PersistHardState(h) = out {
                    model.unsynced_hs.push(h);
                }
                if res.is_err() {
                    inflight = Some(out);
                    break;
                }
                model.log = applied(&model.log, &out);
                model.floor = floor_after(model.floor, &out);
                if let Output::PersistHardState(h) = out {
                    model.hs = h;
                }
            }
            None => {
                if store.sync().is_err() {
                    break;
                }
                model.floor = model.log.len();
                model.synced_hs = model.hs;
                model.unsynced_hs.clear();
            }
        }
    }

    let crashed = store.disk().crash(rng);
    let (_, r) = open(crashed.clone());

    let mut candidates = vec![(model.log.clone(), model.floor)];
    if let Some(out) = &inflight {
        candidates.push((applied(&model.log, out), floor_after(model.floor, out)));
    }
    assert!(
        candidates
            .iter()
            .any(|(log, k)| log.starts_with(&r.log) && r.log.len() >= *k),
        "recovered log {:?} not allowed by {candidates:?}",
        r.log
    );
    assert!(
        r.hard_state == model.synced_hs || model.unsynced_hs.contains(&r.hard_state),
        "recovered {:?}, synced {:?}, unsynced {:?}",
        r.hard_state,
        model.synced_hs,
        model.unsynced_hs
    );

    *model = Model {
        floor: r.log.len(),
        log: r.log,
        hs: r.hard_state,
        synced_hs: r.hard_state,
        unsynced_hs: vec![],
    };
    crashed
}

fn run(seed: u64) {
    let mut rng = Rng(seed);
    let mut model = Model::default();
    let mut disk = CrashDisk::default();
    for _ in 0..4 {
        disk = round(&mut rng, disk, &mut model);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn crash_recovery_matches_synced_state(seed in any::<u64>()) {
        run(seed);
    }
}
