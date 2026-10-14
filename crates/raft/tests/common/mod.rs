//! Black-box harness for the SG2-H suites: drives `Raft` only through its public API and checks
//! Raft's safety properties after every step.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use raft::{
    Config, Entry, Envelope, HardState, Input, LogIndex, Message, NodeId, Output, Payload, Raft,
    Restored, Role, Term,
};

/// Entries `1..=terms.len()` with the given terms. Payloads depend only on the index, so two
/// logs agree exactly where their terms agree.
pub fn entries(terms: &[Term]) -> Vec<Entry> {
    terms.iter().zip(1..).map(|(&t, i)| entry(t, i)).collect()
}

pub fn entry(term: Term, index: LogIndex) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Command(index.to_le_bytes().to_vec()),
    }
}

pub fn noop(term: Term, index: LogIndex) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Noop,
    }
}

pub fn hs(term: Term, voted_for: Option<NodeId>) -> HardState {
    HardState { term, voted_for }
}

pub fn node(id: NodeId, n: u64, hard_state: HardState, terms: &[Term]) -> Raft {
    node_with(id, n, hard_state, terms, id)
}

pub fn node_with(id: NodeId, n: u64, hard_state: HardState, terms: &[Term], entropy: u64) -> Raft {
    let restored = Restored {
        hard_state,
        log: entries(terms),
    };
    Raft::new(Config::new(id, (1..=n).collect()), restored, entropy)
}

pub fn step(r: &mut Raft, input: Input) -> Vec<Output> {
    let mut out = Vec::new();
    r.step(input, &mut out);
    check_output_order(&out);
    out
}

/// D-04: every storage output of a step comes before any `Send` or `Proposed`.
pub fn check_output_order(out: &[Output]) {
    let mut released = false;
    for o in out {
        match o {
            Output::Send(_) | Output::Proposed { .. } => released = true,
            Output::PersistHardState(_) | Output::Truncate { .. } | Output::Append(_) => {
                assert!(!released, "storage output after a send: {out:?}")
            }
            Output::Apply(_) => {}
        }
    }
}

/// Ticks until the node sends something or `limit` ticks pass; returns the tick count and outputs.
pub fn tick_until_send(r: &mut Raft, limit: u32) -> (u32, Vec<Output>) {
    for n in 1..=limit {
        let out = step(r, Input::Tick);
        if out.iter().any(|o| matches!(o, Output::Send(_))) {
            return (n, out);
        }
    }
    panic!("no message after {limit} ticks");
}

pub fn request_vote(
    from: NodeId,
    to: NodeId,
    term: Term,
    last_index: LogIndex,
    last_term: Term,
) -> Input {
    Input::Receive(Envelope {
        from,
        to,
        msg: Message::RequestVote {
            term,
            candidate: from,
            last_log_index: last_index,
            last_log_term: last_term,
        },
    })
}

pub fn vote(from: NodeId, to: NodeId, term: Term, granted: bool) -> Input {
    Input::Receive(Envelope {
        from,
        to,
        msg: Message::RequestVoteResp { term, granted },
    })
}

pub fn append(
    from: NodeId,
    to: NodeId,
    term: Term,
    prev: (LogIndex, Term),
    entries: Vec<Entry>,
    leader_commit: LogIndex,
) -> Input {
    Input::Receive(Envelope {
        from,
        to,
        msg: Message::AppendEntries {
            term,
            leader: from,
            prev_log_index: prev.0,
            prev_log_term: prev.1,
            entries,
            leader_commit,
        },
    })
}

pub fn ack(from: NodeId, to: NodeId, term: Term, match_index: LogIndex) -> Input {
    Input::Receive(Envelope {
        from,
        to,
        msg: Message::AppendEntriesResp {
            term,
            success: true,
            match_index,
            hint: 0,
        },
    })
}

pub fn sent(out: &[Output]) -> Vec<Envelope> {
    out.iter()
        .filter_map(|o| match o {
            Output::Send(env) => Some(env.clone()),
            _ => None,
        })
        .collect()
}

pub fn persisted(out: &[Output]) -> Vec<HardState> {
    out.iter()
        .filter_map(|o| match o {
            Output::PersistHardState(hs) => Some(*hs),
            _ => None,
        })
        .collect()
}

pub fn applied(out: &[Output]) -> Vec<Entry> {
    out.iter()
        .filter_map(|o| match o {
            Output::Apply(es) => Some(es.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Every entry appended across all `Append` outputs, in order.
pub fn appended(out: &[Output]) -> Vec<Entry> {
    out.iter()
        .filter_map(|o| match o {
            Output::Append(es) => Some(es.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

pub fn has_storage_write(out: &[Output]) -> bool {
    out.iter()
        .any(|o| matches!(o, Output::Truncate { .. } | Output::Append(_)))
}

/// The single reply in `out`, which must be an `AppendEntriesResp`: `(term, success, match, hint)`.
pub fn append_resp(out: &[Output]) -> (Term, bool, LogIndex, LogIndex) {
    let replies: Vec<_> = sent(out)
        .into_iter()
        .filter_map(|env| match env.msg {
            Message::AppendEntriesResp {
                term,
                success,
                match_index,
                hint,
            } => Some((term, success, match_index, hint)),
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1, "expected one AppendEntriesResp: {out:?}");
    replies[0]
}

/// The single `RequestVoteResp` in `out`: `(term, granted)`.
pub fn vote_resp(out: &[Output]) -> (Term, bool) {
    let replies: Vec<_> = sent(out)
        .into_iter()
        .filter_map(|env| match env.msg {
            Message::RequestVoteResp { term, granted } => Some((term, granted)),
            _ => None,
        })
        .collect();
    assert_eq!(replies.len(), 1, "expected one RequestVoteResp: {out:?}");
    replies[0]
}

/// Nodes `1..=n` joined by an in-order network the test delivers explicitly. Tracks what each
/// node has durably stored, so nodes can crash and restart, and checks election safety, log
/// matching, leader completeness and state machine safety after every step.
pub struct Cluster {
    pub nodes: BTreeMap<NodeId, Raft>,
    pub net: VecDeque<Envelope>,
    pub isolated: BTreeSet<NodeId>,
    pub durable: BTreeMap<NodeId, Restored>,
    pub leaders: BTreeMap<Term, NodeId>,
    /// Committed entries, each with the lowest term of any node that applied it: an upper bound
    /// on the term it committed in, so every leader of that term or later must hold it.
    pub committed: Vec<(Entry, Term)>,
    pub applied: BTreeMap<NodeId, LogIndex>,
}

impl Cluster {
    pub fn new(init: &[(HardState, &[Term])]) -> Self {
        let n = init.len() as u64;
        let mut c = Cluster {
            nodes: BTreeMap::new(),
            net: VecDeque::new(),
            isolated: BTreeSet::new(),
            durable: BTreeMap::new(),
            leaders: BTreeMap::new(),
            committed: Vec::new(),
            applied: BTreeMap::new(),
        };
        for (id, (hard_state, terms)) in (1..).zip(init) {
            let restored = Restored {
                hard_state: *hard_state,
                log: entries(terms),
            };
            c.durable.insert(id, restored.clone());
            c.nodes.insert(
                id,
                Raft::new(Config::new(id, (1..=n).collect()), restored, id * 7919),
            );
            c.applied.insert(id, 0);
        }
        c
    }

    pub fn fresh(n: u64) -> Self {
        Self::new(&vec![(HardState::default(), &[][..]); n as usize])
    }

    pub fn raft(&self, id: NodeId) -> &Raft {
        &self.nodes[&id]
    }

    pub fn role(&self, id: NodeId) -> Role {
        self.raft(id).status().role
    }

    pub fn log(&self, id: NodeId) -> Vec<Entry> {
        self.raft(id).log().to_vec()
    }

    pub fn step(&mut self, id: NodeId, input: Input) -> Vec<Output> {
        let out = step(self.nodes.get_mut(&id).unwrap(), input);
        for o in &out {
            let d = self.durable.get_mut(&id).unwrap();
            match o {
                Output::PersistHardState(hs) => d.hard_state = *hs,
                Output::Truncate { from } => d.log.truncate((*from - 1) as usize),
                Output::Append(es) => {
                    assert_eq!(es[0].index, d.log.len() as u64 + 1, "append leaves a gap");
                    d.log.extend_from_slice(es);
                }
                Output::Send(env) => {
                    if !self.isolated.contains(&env.from) && !self.isolated.contains(&env.to) {
                        self.net.push_back(env.clone());
                    }
                }
                Output::Apply(es) => {
                    for e in es {
                        let a = self.applied.get_mut(&id).unwrap();
                        *a += 1;
                        assert_eq!(e.index, *a, "node {id} applied out of order");
                        let term = self.nodes[&id].status().term;
                        match self.committed.get_mut(e.index as usize - 1) {
                            Some((c, t)) => {
                                assert_eq!(c, e, "node {id} applied a different entry");
                                *t = (*t).min(term);
                            }
                            None => self.committed.push((e.clone(), term)),
                        }
                    }
                }
                Output::Proposed { .. } => {}
            }
        }
        assert_eq!(
            self.durable[&id].log.as_slice(),
            self.raft(id).log(),
            "node {id}: in-memory log differs from storage outputs"
        );
        self.check_safety(id);
        out
    }

    fn check_safety(&mut self, id: NodeId) {
        let s = self.raft(id).status();
        if s.role == Role::Leader {
            let prev = *self.leaders.entry(s.term).or_insert(id);
            assert_eq!(prev, id, "two leaders in term {}", s.term);
            let log = self.raft(id).log();
            for (e, _) in self.committed.iter().filter(|(_, t)| *t <= s.term) {
                assert_eq!(
                    log.get(e.index as usize - 1),
                    Some(e),
                    "leader {id} of term {} is missing committed entry {}",
                    s.term,
                    e.index
                );
            }
        }
        let mine = self.raft(id).log();
        for (other, r) in &self.nodes {
            let theirs = r.log();
            if let Some(i) = (0..mine.len().min(theirs.len()))
                .rev()
                .find(|&i| mine[i].term == theirs[i].term)
            {
                assert_eq!(
                    mine[..=i],
                    theirs[..=i],
                    "log matching: nodes {id} and {other}"
                );
            }
        }
    }

    pub fn tick(&mut self, id: NodeId) -> Vec<Output> {
        self.step(id, Input::Tick)
    }

    /// Delivers the oldest message; false if the network is empty.
    pub fn deliver_one(&mut self) -> bool {
        self.deliver_nth(0)
    }

    pub fn deliver_nth(&mut self, i: usize) -> bool {
        let Some(env) = self.net.remove(i) else {
            return false;
        };
        self.step(env.to, Input::Receive(env));
        true
    }

    pub fn committed_entries(&self) -> Vec<Entry> {
        self.committed.iter().map(|(e, _)| e.clone()).collect()
    }

    pub fn deliver_all(&mut self) {
        for _ in 0..100_000 {
            if !self.deliver_one() {
                return;
            }
        }
        panic!("network never drained");
    }

    /// Ticks `id` alone until it stands for election, then delivers everything.
    pub fn campaign(&mut self, id: NodeId) {
        for _ in 0..100 {
            self.tick(id);
            if self.role(id) != Role::Follower {
                self.deliver_all();
                return;
            }
        }
        panic!("node {id} never stood for election");
    }

    pub fn elect(&mut self, id: NodeId) {
        self.campaign(id);
        assert_eq!(self.role(id), Role::Leader, "node {id} was not elected");
    }

    /// Ticks the leader until it sends a heartbeat round, then delivers everything.
    pub fn heartbeat(&mut self, leader: NodeId) {
        for _ in 0..100 {
            if !sent(&self.tick(leader)).is_empty() {
                self.deliver_all();
                return;
            }
        }
        panic!("leader {leader} sent no heartbeat");
    }

    pub fn restart(&mut self, id: NodeId, entropy: u64) {
        let cfg = Config::new(id, self.nodes.keys().copied().collect());
        let restored = self.durable[&id].clone();
        self.nodes.insert(id, Raft::new(cfg, restored, entropy));
        self.applied.insert(id, 0);
    }
}
