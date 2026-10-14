use std::collections::BTreeMap;

use raft::NodeId;

pub type ClientId = u64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Op {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Get { key } | Op::Put { key, .. } | Op::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OpResult {
    Value(Option<Vec<u8>>),
    Ok,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    pub client: ClientId,
    pub seq: u64,
    pub op: Op,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRequest {
    pub client: ClientId,
    pub seq: u64,
    pub op: Op,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientReply {
    Ok {
        client: ClientId,
        seq: u64,
        result: OpResult,
    },
    NotLeader {
        client: ClientId,
        seq: u64,
        leader_hint: Option<NodeId>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvState {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    sessions: BTreeMap<ClientId, (u64, OpResult)>,
}

impl KvState {
    pub fn apply(&mut self, cmd: &Command) -> OpResult {
        let _ = (&self.data, &self.sessions, cmd);
        todo!("SG4")
    }
}
