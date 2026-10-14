use kv::{ClientReply, ClientRequest, Command, Op, OpResult};
use raft::{Entry, Envelope, HardState, Message, Payload};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecError {
    Eof,
    BadTag(u8),
    TrailingBytes,
}

pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

pub trait Decode: Sized {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError>;
}

pub fn to_bytes<T: Encode>(v: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    v.encode(&mut buf);
    buf
}

pub fn from_bytes<T: Decode>(bytes: &[u8]) -> Result<T, CodecError> {
    let mut r = Reader(bytes);
    let v = T::decode(&mut r)?;
    if r.0.is_empty() {
        Ok(v)
    } else {
        Err(CodecError::TrailingBytes)
    }
}

pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.0.len() < n {
            return Err(CodecError::Eof);
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn remaining(&self) -> usize {
        self.0.len()
    }
}

impl Encode for u8 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }
}

impl Decode for u8 {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(r.take(1)?[0])
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u64 {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(u64::from_le_bytes(r.take(8)?.try_into().unwrap()))
    }
}

impl Encode for bool {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.push(*self as u8);
    }
}

impl Decode for bool {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(false),
            1 => Ok(true),
            t => Err(CodecError::BadTag(t)),
        }
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            None => buf.push(0),
            Some(v) => {
                buf.push(1);
                v.encode(buf);
            }
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(None),
            1 => Ok(Some(T::decode(r)?)),
            t => Err(CodecError::BadTag(t)),
        }
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, buf: &mut Vec<u8>) {
        let len = u32::try_from(self.len()).expect("vec too long to encode");
        buf.extend_from_slice(&len.to_le_bytes());
        self.iter().for_each(|v| v.encode(buf));
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        let len = u32::from_le_bytes(r.take(4)?.try_into().unwrap()) as usize;
        let mut v = Vec::with_capacity(len.min(r.remaining()));
        for _ in 0..len {
            v.push(T::decode(r)?);
        }
        Ok(v)
    }
}

macro_rules! fields {
    ($buf:ident; $($f:expr),*) => { $( $f.encode($buf); )* };
}

impl Encode for Payload {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Payload::Noop => buf.push(0),
            Payload::Command(data) => {
                buf.push(1);
                data.encode(buf);
            }
        }
    }
}

impl Decode for Payload {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(Payload::Noop),
            1 => Ok(Payload::Command(Decode::decode(r)?)),
            t => Err(CodecError::BadTag(t)),
        }
    }
}

impl Encode for Entry {
    fn encode(&self, buf: &mut Vec<u8>) {
        fields!(buf; self.term, self.index, self.payload);
    }
}

impl Decode for Entry {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Entry {
            term: Decode::decode(r)?,
            index: Decode::decode(r)?,
            payload: Decode::decode(r)?,
        })
    }
}

impl Encode for HardState {
    fn encode(&self, buf: &mut Vec<u8>) {
        fields!(buf; self.term, self.voted_for);
    }
}

impl Decode for HardState {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(HardState {
            term: Decode::decode(r)?,
            voted_for: Decode::decode(r)?,
        })
    }
}

impl Encode for Message {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => {
                buf.push(0);
                fields!(buf; term, candidate, last_log_index, last_log_term);
            }
            Message::RequestVoteResp { term, granted } => {
                buf.push(1);
                fields!(buf; term, granted);
            }
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                buf.push(2);
                fields!(buf; term, leader, prev_log_index, prev_log_term, entries, leader_commit);
            }
            Message::AppendEntriesResp {
                term,
                success,
                match_index,
                hint,
            } => {
                buf.push(3);
                fields!(buf; term, success, match_index, hint);
            }
        }
    }
}

impl Decode for Message {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(match u8::decode(r)? {
            0 => Message::RequestVote {
                term: Decode::decode(r)?,
                candidate: Decode::decode(r)?,
                last_log_index: Decode::decode(r)?,
                last_log_term: Decode::decode(r)?,
            },
            1 => Message::RequestVoteResp {
                term: Decode::decode(r)?,
                granted: Decode::decode(r)?,
            },
            2 => Message::AppendEntries {
                term: Decode::decode(r)?,
                leader: Decode::decode(r)?,
                prev_log_index: Decode::decode(r)?,
                prev_log_term: Decode::decode(r)?,
                entries: Decode::decode(r)?,
                leader_commit: Decode::decode(r)?,
            },
            3 => Message::AppendEntriesResp {
                term: Decode::decode(r)?,
                success: Decode::decode(r)?,
                match_index: Decode::decode(r)?,
                hint: Decode::decode(r)?,
            },
            t => return Err(CodecError::BadTag(t)),
        })
    }
}

impl Encode for Envelope {
    fn encode(&self, buf: &mut Vec<u8>) {
        fields!(buf; self.from, self.to, self.msg);
    }
}

impl Decode for Envelope {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Envelope {
            from: Decode::decode(r)?,
            to: Decode::decode(r)?,
            msg: Decode::decode(r)?,
        })
    }
}

impl Encode for Op {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Op::Get { key } => {
                buf.push(0);
                key.encode(buf);
            }
            Op::Put { key, value } => {
                buf.push(1);
                fields!(buf; key, value);
            }
            Op::Delete { key } => {
                buf.push(2);
                key.encode(buf);
            }
        }
    }
}

impl Decode for Op {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(match u8::decode(r)? {
            0 => Op::Get {
                key: Decode::decode(r)?,
            },
            1 => Op::Put {
                key: Decode::decode(r)?,
                value: Decode::decode(r)?,
            },
            2 => Op::Delete {
                key: Decode::decode(r)?,
            },
            t => return Err(CodecError::BadTag(t)),
        })
    }
}

impl Encode for OpResult {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            OpResult::Value(v) => {
                buf.push(0);
                v.encode(buf);
            }
            OpResult::Ok => buf.push(1),
        }
    }
}

impl Decode for OpResult {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(OpResult::Value(Decode::decode(r)?)),
            1 => Ok(OpResult::Ok),
            t => Err(CodecError::BadTag(t)),
        }
    }
}

impl Encode for Command {
    fn encode(&self, buf: &mut Vec<u8>) {
        fields!(buf; self.client, self.seq, self.op);
    }
}

impl Decode for Command {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Command {
            client: Decode::decode(r)?,
            seq: Decode::decode(r)?,
            op: Decode::decode(r)?,
        })
    }
}

impl Encode for ClientRequest {
    fn encode(&self, buf: &mut Vec<u8>) {
        fields!(buf; self.client, self.seq, self.op);
    }
}

impl Decode for ClientRequest {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(ClientRequest {
            client: Decode::decode(r)?,
            seq: Decode::decode(r)?,
            op: Decode::decode(r)?,
        })
    }
}

impl Encode for ClientReply {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            ClientReply::Ok {
                client,
                seq,
                result,
            } => {
                buf.push(0);
                fields!(buf; client, seq, result);
            }
            ClientReply::NotLeader {
                client,
                seq,
                leader_hint,
            } => {
                buf.push(1);
                fields!(buf; client, seq, leader_hint);
            }
        }
    }
}

impl Decode for ClientReply {
    fn decode(r: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(match u8::decode(r)? {
            0 => ClientReply::Ok {
                client: Decode::decode(r)?,
                seq: Decode::decode(r)?,
                result: Decode::decode(r)?,
            },
            1 => ClientReply::NotLeader {
                client: Decode::decode(r)?,
                seq: Decode::decode(r)?,
                leader_hint: Decode::decode(r)?,
            },
            t => return Err(CodecError::BadTag(t)),
        })
    }
}
