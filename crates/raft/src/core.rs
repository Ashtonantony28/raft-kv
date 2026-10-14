use std::collections::{BTreeMap, BTreeSet};

use crate::{
    Config, Entry, Envelope, HardState, Input, LogIndex, Message, NodeId, NotLeader, Output,
    Payload, Restored, Role, Status, Term,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Progress {
    pub(crate) next: LogIndex,
    pub(crate) matched: LogIndex,
}

#[derive(Clone, Debug)]
pub struct Raft {
    pub(crate) cfg: Config,
    pub(crate) hard_state: HardState,
    pub(crate) log: Vec<Entry>,
    pub(crate) role: Role,
    pub(crate) leader: Option<NodeId>,
    pub(crate) commit: LogIndex,
    pub(crate) votes: BTreeSet<NodeId>,
    pub(crate) progress: BTreeMap<NodeId, Progress>,
    pub(crate) election_elapsed: u32,
    pub(crate) election_timeout: u32,
    pub(crate) heartbeat_elapsed: u32,
    pub(crate) rng: u64,
    pub(crate) pending: Vec<Output>,
}

impl Raft {
    pub fn new(cfg: Config, restored: Restored, entropy: u64) -> Self {
        let mut raft = Raft {
            cfg,
            hard_state: restored.hard_state,
            log: restored.log,
            role: Role::Follower,
            leader: None,
            commit: 0,
            votes: BTreeSet::new(),
            progress: BTreeMap::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng: entropy,
            pending: Vec::new(),
        };
        raft.reset_election_timer();
        raft
    }

    /// Storage outputs are moved ahead of everything else so the driver can sync them
    /// before any message or proposal result leaves the node (D-04).
    pub fn step(&mut self, input: Input, out: &mut Vec<Output>) {
        match input {
            Input::Tick => self.tick(),
            Input::Receive(env) => self.receive(env),
            Input::Propose { tag, data } => self.propose(tag, data),
        }
        let (storage, rest): (Vec<_>, Vec<_>) = self.pending.drain(..).partition(is_storage);
        out.extend(storage);
        out.extend(rest);
    }

    pub fn status(&self) -> Status {
        Status {
            id: self.cfg.id,
            role: self.role,
            term: self.hard_state.term,
            leader: self.leader,
            commit: self.commit,
            last_index: self.last_log_index(),
            last_term: self.last_log_term(),
        }
    }

    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    fn tick(&mut self) {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                self.on_election_timeout();
            }
        }
    }

    fn receive(&mut self, Envelope { from, to, msg }: Envelope) {
        if to != self.cfg.id || from == self.cfg.id || !self.cfg.peers.contains(&from) {
            return;
        }
        match msg {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } if candidate == from => {
                self.handle_request_vote(candidate, term, last_log_index, last_log_term)
            }
            Message::RequestVoteResp { term, granted } => {
                self.handle_request_vote_resp(from, term, granted)
            }
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } if leader == from && well_formed(term, prev_log_index, prev_log_term, &entries) => {
                self.handle_append_entries(
                    leader,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
            }
            Message::AppendEntriesResp {
                term,
                success,
                match_index,
                hint,
            } => self.handle_append_entries_resp(from, term, success, match_index, hint),
            _ => {}
        }
    }

    fn propose(&mut self, tag: u64, data: Vec<u8>) {
        if self.role != Role::Leader {
            let result = Err(NotLeader {
                leader_hint: self.leader,
            });
            self.pending.push(Output::Proposed { tag, result });
            return;
        }
        let index = self.last_log_index() + 1;
        self.append_local(vec![Entry {
            term: self.hard_state.term,
            index,
            payload: Payload::Command(data),
        }]);
        self.pending.push(Output::Proposed {
            tag,
            result: Ok(index),
        });
        self.maybe_commit();
        self.broadcast_append();
    }

    fn handle_append_entries_resp(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        hint: LogIndex,
    ) {
        if term > self.hard_state.term {
            self.become_follower(term, None);
            return;
        }
        if term < self.hard_state.term || self.role != Role::Leader {
            return;
        }
        let last = self.last_log_index();
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        if success {
            if match_index > last {
                return;
            }
            pr.matched = pr.matched.max(match_index);
            pr.next = pr.next.max(match_index + 1);
            let more = pr.next <= last;
            self.maybe_commit();
            if more {
                self.send_append(from);
            }
        } else {
            let next = (pr.next - 1).min(hint).max(pr.matched + 1);
            if next != pr.next {
                pr.next = next;
                self.send_append(from);
            }
        }
    }

    fn maybe_commit(&mut self) {
        let last = self.last_log_index();
        let mut matched: Vec<LogIndex> = self
            .cfg
            .peers
            .iter()
            .map(|id| match self.progress.get(id) {
                _ if *id == self.cfg.id => last,
                Some(pr) => pr.matched,
                None => 0,
            })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&n) = matched.get(self.cfg.quorum() - 1) else {
            return;
        };
        // Only entries from the current term commit by counting replicas (§5.4.2).
        if self.term_at(n) == Some(self.hard_state.term) {
            self.commit_to(n);
        }
    }

    fn broadcast_append(&mut self) {
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for to in peers {
            self.send_append(to);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let Some(pr) = self.progress.get(&to) else {
            return;
        };
        let prev = pr.next - 1;
        let start = prev as usize;
        let end = (start + self.cfg.max_append_entries.max(1)).min(self.log.len());
        let msg = Message::AppendEntries {
            term: self.hard_state.term,
            leader: self.cfg.id,
            prev_log_index: prev,
            prev_log_term: self.term_at(prev).unwrap_or(0),
            entries: self.log[start..end].to_vec(),
            leader_commit: self.commit,
        };
        self.send(to, msg);
    }

    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    // Helpers for SG2-H (election.rs, log_matching.rs).

    /// Adopts `term` (persisting it and clearing the vote if it is newer) and becomes a follower.
    pub(crate) fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        debug_assert!(term >= self.hard_state.term, "term went backwards");
        if term > self.hard_state.term {
            self.hard_state = HardState {
                term,
                voted_for: None,
            };
            self.persist_hard_state();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.progress.clear();
        self.reset_election_timer();
    }

    /// Becomes leader of the current term: initialises next/match, appends a Noop and broadcasts it.
    #[cfg_attr(not(test), expect(dead_code, reason = "called by SG2-H"))]
    pub(crate) fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.votes.clear();
        self.heartbeat_elapsed = 0;
        let next = self.last_log_index() + 1;
        let id = self.cfg.id;
        self.progress = self
            .cfg
            .peers
            .iter()
            .filter(|&&p| p != id)
            .map(|&p| (p, Progress { next, matched: 0 }))
            .collect();
        self.append_local(vec![Entry {
            term: self.hard_state.term,
            index: next,
            payload: Payload::Noop,
        }]);
        self.maybe_commit();
        self.broadcast_append();
    }

    /// Restarts the election timer with a fresh timeout drawn from `[min, max)` election ticks.
    pub(crate) fn reset_election_timer(&mut self) {
        let (lo, hi) = self.cfg.election_ticks;
        let span = u64::from(hi.saturating_sub(lo).max(1));
        self.election_elapsed = 0;
        self.election_timeout = lo + (self.next_rand() % span) as u32;
    }

    pub(crate) fn persist_hard_state(&mut self) {
        self.pending.push(Output::PersistHardState(self.hard_state));
    }

    pub(crate) fn last_log_index(&self) -> LogIndex {
        self.log.last().map_or(0, |e| e.index)
    }

    pub(crate) fn last_log_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }

    /// Term of the entry at `index`; `Some(0)` for index 0, `None` past the end of the log.
    pub(crate) fn term_at(&self, index: LogIndex) -> Option<Term> {
        match index {
            0 => Some(0),
            i => self.log.get((i - 1) as usize).map(|e| e.term),
        }
    }

    /// Appends entries that directly follow the last log entry.
    pub(crate) fn append_local(&mut self, entries: Vec<Entry>) {
        let Some(first) = entries.first() else {
            return;
        };
        debug_assert_eq!(
            first.index,
            self.last_log_index() + 1,
            "non-contiguous append"
        );
        self.log.extend_from_slice(&entries);
        self.pending.push(Output::Append(entries));
    }

    /// Deletes entries with index >= `from`. Truncating a committed entry is a bug.
    #[cfg_attr(not(test), expect(dead_code, reason = "called by SG2-H"))]
    pub(crate) fn truncate_local(&mut self, from: LogIndex) {
        debug_assert!(from > self.commit, "truncating committed entry {from}");
        if from > self.last_log_index() {
            return;
        }
        self.log.truncate((from - 1) as usize);
        self.pending.push(Output::Truncate { from });
    }

    /// Raises the commit index to `index` (capped at the last log index) and emits `Apply`.
    pub(crate) fn commit_to(&mut self, index: LogIndex) {
        let index = index.min(self.last_log_index());
        if index <= self.commit {
            return;
        }
        let entries = self.log[self.commit as usize..index as usize].to_vec();
        self.commit = index;
        self.pending.push(Output::Apply(entries));
    }

    pub(crate) fn send(&mut self, to: NodeId, msg: Message) {
        self.pending.push(Output::Send(Envelope {
            from: self.cfg.id,
            to,
            msg,
        }));
    }
}

pub(crate) fn is_storage(out: &Output) -> bool {
    matches!(
        out,
        Output::PersistHardState(_) | Output::Truncate { .. } | Output::Append(_)
    )
}

/// Entries must be contiguous after `prev_index` with non-decreasing terms no newer than `term`.
fn well_formed(term: Term, prev_index: LogIndex, prev_term: Term, entries: &[Entry]) -> bool {
    let mut prev = (prev_index, prev_term);
    prev_term <= term
        && entries.iter().all(|e| {
            let ok = prev.0.checked_add(1) == Some(e.index) && prev.1 <= e.term && e.term <= term;
            prev = (e.index, e.term);
            ok
        })
}
