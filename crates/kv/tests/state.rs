use std::collections::BTreeMap;

use kv::{ClientId, Command, KvState, Op, OpResult};
use proptest::prelude::*;

fn cmd(client: ClientId, seq: u64, op: Op) -> Command {
    Command { client, seq, op }
}

fn put(k: &str, v: &str) -> Op {
    Op::Put {
        key: k.into(),
        value: v.into(),
    }
}

fn get(k: &str) -> Op {
    Op::Get { key: k.into() }
}

fn del(k: &str) -> Op {
    Op::Delete { key: k.into() }
}

fn val(v: &str) -> OpResult {
    OpResult::Value(Some(v.into()))
}

#[test]
fn put_get_delete() {
    let mut s = KvState::default();
    assert_eq!(s.apply(&cmd(1, 1, get("a"))), OpResult::Value(None));
    assert_eq!(s.apply(&cmd(1, 2, put("a", "x"))), OpResult::Ok);
    assert_eq!(s.apply(&cmd(1, 3, get("a"))), val("x"));
    assert_eq!(s.apply(&cmd(1, 4, put("a", "y"))), OpResult::Ok);
    assert_eq!(s.apply(&cmd(2, 1, get("a"))), val("y"));
    assert_eq!(s.apply(&cmd(1, 5, del("a"))), OpResult::Ok);
    assert_eq!(s.apply(&cmd(1, 6, del("a"))), OpResult::Ok);
    assert_eq!(s.apply(&cmd(2, 2, get("a"))), OpResult::Value(None));
    assert_eq!(s.get(b"a"), None);
}

#[test]
fn duplicate_returns_cached_without_reapplying() {
    let mut s = KvState::default();
    s.apply(&cmd(1, 1, put("a", "x")));
    s.apply(&cmd(2, 1, put("a", "y")));
    assert_eq!(s.apply(&cmd(1, 1, put("a", "x"))), OpResult::Ok);
    assert_eq!(s.get(b"a"), Some(&b"y"[..]));
}

#[test]
fn duplicate_get_returns_original_read() {
    let mut s = KvState::default();
    s.apply(&cmd(1, 1, put("a", "x")));
    assert_eq!(s.apply(&cmd(2, 1, get("a"))), val("x"));
    s.apply(&cmd(1, 2, put("a", "y")));
    assert_eq!(s.apply(&cmd(2, 1, get("a"))), val("x"));
    assert_eq!(s.session(2), Some((1, &val("x"))));
}

#[test]
fn stale_seq_is_not_applied() {
    let mut s = KvState::default();
    s.apply(&cmd(1, 5, put("a", "x")));
    assert_eq!(s.apply(&cmd(1, 3, put("a", "stale"))), OpResult::Ok);
    assert_eq!(s.apply(&cmd(1, 4, get("a"))), OpResult::Ok);
    assert_eq!(s.get(b"a"), Some(&b"x"[..]));
    assert_eq!(s.session(1), Some((5, &OpResult::Ok)));
}

#[test]
fn sessions_are_per_client() {
    let mut s = KvState::default();
    s.apply(&cmd(1, 1, put("a", "x")));
    assert_eq!(s.apply(&cmd(2, 1, put("a", "y"))), OpResult::Ok);
    assert_eq!(s.get(b"a"), Some(&b"y"[..]));
    assert_eq!(s.sessions().count(), 2);
    assert_eq!(s.session(3), None);
}

#[test]
fn seq_gaps_are_accepted() {
    let mut s = KvState::default();
    s.apply(&cmd(1, 1, put("a", "x")));
    assert_eq!(s.apply(&cmd(1, 10, get("a"))), val("x"));
    assert_eq!(s.session(1), Some((10, &val("x"))));
}

fn op() -> impl Strategy<Value = Op> {
    let key = (0u8..4).prop_map(|k| vec![k]);
    let value = prop::collection::vec(any::<u8>(), 0..4);
    prop_oneof![
        key.clone().prop_map(|key| Op::Get { key }),
        (key.clone(), value).prop_map(|(key, value)| Op::Put { key, value }),
        key.prop_map(|key| Op::Delete { key }),
    ]
}

// Mostly fresh seqs, with retries of the last seq and stale seqs mixed in.
fn commands() -> impl Strategy<Value = Vec<Command>> {
    prop::collection::vec((0u64..3, 0u8..10, op()), 0..200).prop_map(|steps| {
        let mut next = BTreeMap::<ClientId, u64>::new();
        let mut out = Vec::new();
        for (client, kind, op) in steps {
            let n = next.entry(client).or_insert(1);
            let seq = match kind {
                0..=6 => {
                    *n += 1;
                    *n - 1
                }
                7 | 8 => n.saturating_sub(1),
                _ => n.saturating_sub(3),
            };
            out.push(Command { client, seq, op });
        }
        out
    })
}

#[derive(Default)]
struct Model {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    last: BTreeMap<ClientId, (u64, OpResult)>,
}

impl Model {
    fn apply(&mut self, c: &Command) -> OpResult {
        if let Some((seq, r)) = self.last.get(&c.client)
            && c.seq <= *seq
        {
            return r.clone();
        }
        let r = match &c.op {
            Op::Get { key } => OpResult::Value(self.data.get(key).cloned()),
            Op::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                OpResult::Ok
            }
            Op::Delete { key } => {
                self.data.remove(key);
                OpResult::Ok
            }
        };
        self.last.insert(c.client, (c.seq, r.clone()));
        r
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn matches_btreemap_model(cmds in commands()) {
        let mut s = KvState::default();
        let mut m = Model::default();
        for c in &cmds {
            prop_assert_eq!(s.apply(c), m.apply(c));
        }
        let data: Vec<_> = m.data.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        prop_assert_eq!(s.data().collect::<Vec<_>>(), data);
        let sessions: Vec<_> = m.last.iter().map(|(c, (q, r))| (*c, *q, r)).collect();
        prop_assert_eq!(s.sessions().collect::<Vec<_>>(), sessions);
    }

    #[test]
    fn redelivery_returns_cached_and_changes_nothing(cmds in commands()) {
        let mut s = KvState::default();
        cmds.iter().for_each(|c| { s.apply(c); });
        let after = s.clone();
        for c in &cmds {
            prop_assert_eq!(&s.apply(c), after.session(c.client).unwrap().1);
        }
        prop_assert_eq!(s, after);
    }
}
