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
        if let Some((last, cached)) = self.sessions.get(&cmd.client)
            && cmd.seq <= *last
        {
            return cached.clone();
        }
        let result = match &cmd.op {
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
        self.sessions.insert(cmd.client, (cmd.seq, result.clone()));
        result
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(Vec::as_slice)
    }

    pub fn session(&self, client: ClientId) -> Option<(u64, &OpResult)> {
        self.sessions.get(&client).map(|(seq, r)| (*seq, r))
    }

    pub fn data(&self) -> impl ExactSizeIterator<Item = (&[u8], &[u8])> {
        self.data.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    pub fn sessions(&self) -> impl ExactSizeIterator<Item = (ClientId, u64, &OpResult)> {
        self.sessions.iter().map(|(c, (s, r))| (*c, *s, r))
    }
}
