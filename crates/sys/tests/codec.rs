use kv::{ClientReply, ClientRequest, Command, Op, OpResult};
use proptest::prelude::*;
use raft::{Entry, Envelope, HardState, Message, Payload};
use sys::codec::{CodecError, Decode, Encode, from_bytes, to_bytes};

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..16)
}

fn payload() -> impl Strategy<Value = Payload> {
    prop_oneof![Just(Payload::Noop), bytes().prop_map(Payload::Command)]
}

fn entry() -> impl Strategy<Value = Entry> {
    (any::<u64>(), any::<u64>(), payload()).prop_map(|(term, index, payload)| Entry {
        term,
        index,
        payload,
    })
}

fn message() -> impl Strategy<Value = Message> {
    prop_oneof![
        any::<[u64; 4]>().prop_map(|[term, candidate, last_log_index, last_log_term]| {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            }
        }),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, granted)| Message::RequestVoteResp { term, granted }),
        (any::<[u64; 5]>(), prop::collection::vec(entry(), 0..8)).prop_map(
            |([term, leader, prev_log_index, prev_log_term, leader_commit], entries)| {
                Message::AppendEntries {
                    term,
                    leader,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                }
            }
        ),
        (any::<[u64; 3]>(), any::<bool>()).prop_map(|([term, match_index, hint], success)| {
            Message::AppendEntriesResp {
                term,
                success,
                match_index,
                hint,
            }
        }),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        bytes().prop_map(|key| Op::Get { key }),
        (bytes(), bytes()).prop_map(|(key, value)| Op::Put { key, value }),
        bytes().prop_map(|key| Op::Delete { key }),
    ]
}

fn op_result() -> impl Strategy<Value = OpResult> {
    prop_oneof![
        Just(OpResult::Ok),
        prop::option::of(bytes()).prop_map(OpResult::Value)
    ]
}

fn client_reply() -> impl Strategy<Value = ClientReply> {
    prop_oneof![
        (any::<u64>(), any::<u64>(), op_result()).prop_map(|(client, seq, result)| {
            ClientReply::Ok {
                client,
                seq,
                result,
            }
        }),
        (any::<u64>(), any::<u64>(), any::<Option<u64>>()).prop_map(
            |(client, seq, leader_hint)| ClientReply::NotLeader {
                client,
                seq,
                leader_hint,
            }
        ),
    ]
}

fn roundtrip<T: Encode + Decode + PartialEq + std::fmt::Debug>(v: T) {
    let bytes = to_bytes(&v);
    assert_eq!(from_bytes::<T>(&bytes), Ok(v));
    if !bytes.is_empty() {
        assert!(from_bytes::<T>(&bytes[..bytes.len() - 1]).is_err());
    }
    let mut extra = bytes;
    extra.push(0);
    assert_eq!(from_bytes::<T>(&extra), Err(CodecError::TrailingBytes));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn envelope(from: u64, to: u64, msg in message()) {
        roundtrip(Envelope { from, to, msg });
    }

    #[test]
    fn hard_state(term: u64, voted_for: Option<u64>) {
        roundtrip(HardState { term, voted_for });
    }

    #[test]
    fn entries(es in prop::collection::vec(entry(), 0..8)) {
        roundtrip(es);
    }

    #[test]
    fn command(client: u64, seq: u64, op in op()) {
        roundtrip(Command { client, seq, op });
    }

    #[test]
    fn client_request(client: u64, seq: u64, op in op()) {
        roundtrip(ClientRequest { client, seq, op });
    }

    #[test]
    fn client_replies(r in client_reply()) {
        roundtrip(r);
    }

    #[test]
    fn garbage_never_panics(b in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = from_bytes::<Envelope>(&b);
        let _ = from_bytes::<ClientRequest>(&b);
        let _ = from_bytes::<ClientReply>(&b);
        let _ = from_bytes::<Vec<Entry>>(&b);
    }
}

#[test]
fn huge_length_prefix_does_not_allocate() {
    let b = [0xff, 0xff, 0xff, 0xff];
    assert_eq!(from_bytes::<Vec<Entry>>(&b), Err(CodecError::Eof));
}

#[test]
fn golden_encoding() {
    let env = Envelope {
        from: 1,
        to: 2,
        msg: Message::RequestVoteResp {
            term: 3,
            granted: true,
        },
    };
    assert_eq!(
        to_bytes(&env),
        [
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 1
        ]
    );
}
