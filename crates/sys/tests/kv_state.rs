use kv::{Command, KvState, Op, OpResult};
use proptest::prelude::*;
use sys::codec::to_bytes;

fn command() -> impl Strategy<Value = Command> {
    let key = (0u8..4).prop_map(|k| vec![k]);
    let op = prop_oneof![
        key.clone().prop_map(|key| Op::Get { key }),
        (key.clone(), prop::collection::vec(any::<u8>(), 0..4))
            .prop_map(|(key, value)| Op::Put { key, value }),
        key.prop_map(|key| Op::Delete { key }),
    ];
    (0u64..3, 0u64..20, op).prop_map(|(client, seq, op)| Command { client, seq, op })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn apply_is_deterministic(cmds in prop::collection::vec(command(), 0..200)) {
        let run = || {
            let mut s = KvState::default();
            let results: Vec<OpResult> = cmds.iter().map(|c| s.apply(c)).collect();
            (s, results)
        };
        let (a, ra) = run();
        let (b, rb) = run();
        prop_assert_eq!(ra, rb);
        prop_assert_eq!(to_bytes(&a), to_bytes(&b));
        prop_assert_eq!(a, b);
    }
}

#[test]
fn encoding_is_stable() {
    let mut s = KvState::default();
    s.apply(&Command {
        client: 7,
        seq: 1,
        op: Op::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        },
    });
    s.apply(&Command {
        client: 3,
        seq: 2,
        op: Op::Get { key: b"k".to_vec() },
    });
    #[rustfmt::skip]
    let expected: Vec<u8> = [
        &[1, 0, 0, 0, 0, 0, 0, 0][..],          // 1 key
        &[1, 0, 0, 0, b'k', 1, 0, 0, 0, b'v'],  // "k" => "v"
        &[2, 0, 0, 0, 0, 0, 0, 0],              // 2 sessions
        &[3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, b'v'],
        &[7, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1],
    ]
    .concat();
    assert_eq!(to_bytes(&s), expected);
}
