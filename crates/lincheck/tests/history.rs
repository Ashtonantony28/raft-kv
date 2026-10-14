use kv::{Op, OpResult};
use lincheck::{History, Operation};

fn sample() -> History {
    History {
        ops: vec![
            Operation {
                client: 1,
                op: Op::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                call: 10,
                ret: Some((25, OpResult::Ok)),
            },
            Operation {
                client: 2,
                op: Op::Get { key: b"k".to_vec() },
                call: 12,
                ret: Some((30, OpResult::Value(Some(b"v".to_vec())))),
            },
            Operation {
                client: 3,
                op: Op::Delete { key: b"k".to_vec() },
                call: 40,
                ret: None,
            },
        ],
    }
}

const GOLDEN: &str = r#"{"client":1,"op":{"Put":{"key":[107],"value":[118]}},"call":10,"ret":[25,"Ok"]}
{"client":2,"op":{"Get":{"key":[107]}},"call":12,"ret":[30,{"Value":[118]}]}
{"client":3,"op":{"Delete":{"key":[107]}},"call":40,"ret":null}
"#;

#[test]
fn jsonl_golden() {
    let mut out = Vec::new();
    sample().write_jsonl(&mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), GOLDEN);
}

#[test]
fn jsonl_roundtrip() {
    let h = History::read_jsonl(GOLDEN.as_bytes()).unwrap();
    assert_eq!(h, sample());
}

#[test]
fn jsonl_rejects_garbage() {
    assert!(History::read_jsonl(&b"{\"client\":1}\n"[..]).is_err());
}
