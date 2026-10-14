mod core;
mod election;
mod log_matching;
#[cfg(test)]
mod tests;

pub use core::Raft;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Noop,
    Command(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteResp {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        leader: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    },
    AppendEntriesResp {
        term: Term,
        success: bool,
        match_index: LogIndex,
        hint: LogIndex,
    },
}

impl Message {
    pub fn term(&self) -> Term {
        match *self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResp { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResp { term, .. } => term,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_ticks: (u32, u32),
    pub heartbeat_ticks: u32,
    pub max_append_entries: usize,
}

impl Config {
    pub fn new(id: NodeId, mut peers: Vec<NodeId>) -> Self {
        peers.sort_unstable();
        peers.dedup();
        Config {
            id,
            peers,
            election_ticks: (10, 20),
            heartbeat_ticks: 2,
            max_append_entries: 64,
        }
    }

    pub fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Restored {
    pub hard_state: HardState,
    pub log: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Tick,
    Receive(Envelope),
    Propose { tag: u64, data: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    PersistHardState(HardState),
    Truncate {
        from: LogIndex,
    },
    Append(Vec<Entry>),
    Send(Envelope),
    Apply(Vec<Entry>),
    Proposed {
        tag: u64,
        result: Result<LogIndex, NotLeader>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotLeader {
    pub leader_hint: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit: LogIndex,
    pub last_index: LogIndex,
    pub last_term: Term,
}
