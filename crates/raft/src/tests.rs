//! SG2 unit tests. Leaders are built with `become_leader` directly so nothing here depends on
//! SG2-H code; the SG2-H suites live in `tests/`.

use crate::core::Progress;
use crate::{
    Config, Entry, Envelope, HardState, Input, LogIndex, Message, NodeId, NotLeader, Output,
    Payload, Raft, Restored, Role, Term,
};

fn log(terms: &[Term]) -> Vec<Entry> {
    terms
        .iter()
        .zip(1..)
        .map(|(&term, index)| Entry {
            term,
            index,
            payload: Payload::Command(vec![index as u8]),
        })
        .collect()
}

fn node(id: NodeId, n: u64, term: Term, terms: &[Term]) -> Raft {
    let restored = Restored {
        hard_state: HardState {
            term,
            voted_for: None,
        },
        log: log(terms),
    };
    Raft::new(Config::new(id, (1..=n).collect()), restored, 7)
}

fn leader(n: u64, term: Term, terms: &[Term]) -> (Raft, Vec<Output>) {
    let mut r = node(1, n, term, terms);
    r.become_leader();
    let out = drain(&mut r);
    (r, out)
}

fn drain(r: &mut Raft) -> Vec<Output> {
    std::mem::take(&mut r.pending)
}

fn step(r: &mut Raft, input: Input) -> Vec<Output> {
    let mut out = Vec::new();
    r.step(input, &mut out);
    out
}

fn ack(from: NodeId, term: Term, success: bool, match_index: LogIndex, hint: LogIndex) -> Input {
    Input::Receive(Envelope {
        from,
        to: 1,
        msg: Message::AppendEntriesResp {
            term,
            success,
            match_index,
            hint,
        },
    })
}

fn appends(out: &[Output]) -> Vec<(NodeId, LogIndex, Term, Vec<Entry>, LogIndex)> {
    out.iter()
        .filter_map(|o| match o {
            Output::Send(Envelope {
                to,
                msg:
                    Message::AppendEntries {
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        ..
                    },
                ..
            }) => Some((
                *to,
                *prev_log_index,
                *prev_log_term,
                entries.clone(),
                *leader_commit,
            )),
            _ => None,
        })
        .collect()
}

fn applied(out: &[Output]) -> Vec<LogIndex> {
    out.iter()
        .filter_map(|o| match o {
            Output::Apply(es) => Some(es.iter().map(|e| e.index)),
            _ => None,
        })
        .flatten()
        .collect()
}

fn noop(term: Term, index: LogIndex) -> Entry {
    Entry {
        term,
        index,
        payload: Payload::Noop,
    }
}

#[test]
fn new_restores_state_as_follower() {
    let r = node(2, 3, 4, &[1, 3, 3]);
    let s = r.status();
    assert_eq!(
        (s.id, s.role, s.term, s.leader),
        (2, Role::Follower, 4, None)
    );
    assert_eq!((s.commit, s.last_index, s.last_term), (0, 3, 3));
    assert_eq!(r.log(), log(&[1, 3, 3]).as_slice());
}

#[test]
fn election_timeout_is_in_range_and_seeded() {
    let mut seen = std::collections::BTreeSet::new();
    for entropy in 0..200 {
        let mut a = Raft::new(Config::new(1, vec![1, 2, 3]), Restored::default(), entropy);
        let b = Raft::new(Config::new(1, vec![1, 2, 3]), Restored::default(), entropy);
        assert_eq!(a.election_timeout, b.election_timeout);
        for _ in 0..5 {
            assert!((10..20).contains(&a.election_timeout));
            seen.insert(a.election_timeout);
            a.reset_election_timer();
            assert_eq!(a.election_elapsed, 0);
        }
    }
    assert_eq!(seen.len(), 10, "every timeout in [10, 20) should occur");
}

#[test]
fn degenerate_election_range_uses_min() {
    let mut cfg = Config::new(1, vec![1, 2, 3]);
    cfg.election_ticks = (5, 5);
    assert_eq!(Raft::new(cfg, Restored::default(), 3).election_timeout, 5);
}

#[test]
fn follower_is_silent_before_timeout() {
    let mut r = node(1, 3, 1, &[1]);
    for _ in 1..r.election_timeout {
        assert!(step(&mut r, Input::Tick).is_empty());
    }
    assert_eq!(r.status().role, Role::Follower);
}

#[test]
fn become_follower_persists_only_newer_terms() {
    let mut r = node(1, 3, 2, &[]);
    r.hard_state.voted_for = Some(3);
    r.become_follower(2, Some(2));
    assert!(drain(&mut r).is_empty());
    assert_eq!(r.hard_state.voted_for, Some(3));
    assert_eq!(r.status().leader, Some(2));

    r.election_elapsed = 5;
    r.become_follower(4, None);
    assert_eq!(
        drain(&mut r),
        vec![Output::PersistHardState(HardState {
            term: 4,
            voted_for: None
        })]
    );
    assert_eq!((r.status().term, r.status().leader), (4, None));
    assert_eq!(r.election_elapsed, 0);
}

#[test]
fn become_leader_appends_noop_and_broadcasts() {
    let (r, out) = leader(3, 2, &[1, 1]);
    assert_eq!(out[0], Output::Append(vec![noop(2, 3)]));
    assert_eq!(
        appends(&out),
        vec![
            (2, 2, 1, vec![noop(2, 3)], 0),
            (3, 2, 1, vec![noop(2, 3)], 0)
        ]
    );
    let s = r.status();
    assert_eq!(
        (s.role, s.leader, s.last_index, s.last_term),
        (Role::Leader, Some(1), 3, 2)
    );
    assert_eq!(
        r.progress[&2],
        Progress {
            next: 3,
            matched: 0
        }
    );
    assert!(!r.progress.contains_key(&1));
}

#[test]
fn single_node_leader_commits_immediately() {
    let (mut r, out) = leader(1, 1, &[]);
    assert_eq!(applied(&out), vec![1]);
    let out = step(
        &mut r,
        Input::Propose {
            tag: 9,
            data: vec![42],
        },
    );
    assert!(out.contains(&Output::Proposed {
        tag: 9,
        result: Ok(2)
    }));
    assert_eq!(applied(&out), vec![2]);
    assert_eq!(r.status().commit, 2);
}

#[test]
fn leader_heartbeats_every_heartbeat_ticks_and_never_times_out() {
    let (mut r, _) = leader(3, 1, &[]);
    for i in 1..=100 {
        let out = step(&mut r, Input::Tick);
        let sent: Vec<NodeId> = appends(&out).iter().map(|a| a.0).collect();
        if i % 2 == 0 {
            assert_eq!(sent, vec![2, 3], "tick {i}");
        } else {
            assert!(out.is_empty(), "tick {i}");
        }
    }
    assert_eq!(r.status().role, Role::Leader);
}

#[test]
fn heartbeat_retries_unacked_entries() {
    let (mut r, _) = leader(3, 1, &[]);
    step(&mut r, ack(2, 1, true, 1, 0));
    step(&mut r, Input::Tick);
    let out = step(&mut r, Input::Tick);
    assert_eq!(
        appends(&out),
        vec![(2, 1, 1, vec![], 1), (3, 0, 0, vec![noop(1, 1)], 1)]
    );
}

#[test]
fn propose_on_leader_appends_then_replicates() {
    let (mut r, _) = leader(3, 2, &[1]);
    let out = step(
        &mut r,
        Input::Propose {
            tag: 5,
            data: vec![7],
        },
    );
    let entry = Entry {
        term: 2,
        index: 3,
        payload: Payload::Command(vec![7]),
    };
    assert_eq!(out[0], Output::Append(vec![entry.clone()]));
    assert_eq!(
        out[1],
        Output::Proposed {
            tag: 5,
            result: Ok(3)
        }
    );
    assert_eq!(
        appends(&out),
        vec![
            (2, 1, 1, vec![noop(2, 2), entry.clone()], 0),
            (3, 1, 1, vec![noop(2, 2), entry], 0)
        ]
    );
    assert!(applied(&out).is_empty());
}

#[test]
fn propose_when_not_leader_returns_hint() {
    let mut r = node(1, 3, 1, &[]);
    let out = step(
        &mut r,
        Input::Propose {
            tag: 1,
            data: vec![],
        },
    );
    let not_leader = |leader_hint| Output::Proposed {
        tag: 1,
        result: Err(NotLeader { leader_hint }),
    };
    assert_eq!(out, vec![not_leader(None)]);
    r.leader = Some(3);
    assert_eq!(
        step(
            &mut r,
            Input::Propose {
                tag: 1,
                data: vec![]
            }
        ),
        vec![not_leader(Some(3))]
    );
    r.role = Role::Candidate;
    r.leader = None;
    assert_eq!(
        step(
            &mut r,
            Input::Propose {
                tag: 1,
                data: vec![]
            }
        ),
        vec![not_leader(None)]
    );
    assert!(r.log().is_empty());
}

#[test]
fn ack_from_quorum_commits_and_applies() {
    let (mut r, _) = leader(3, 2, &[1, 1]);
    let out = step(&mut r, ack(2, 2, true, 3, 0));
    assert_eq!(applied(&out), vec![1, 2, 3]);
    assert_eq!(r.status().commit, 3);
    assert_eq!(
        r.progress[&2],
        Progress {
            next: 4,
            matched: 3
        }
    );
    assert!(appends(&out).is_empty(), "nothing left to send");
}

#[test]
fn old_term_entries_commit_only_with_a_current_term_entry() {
    let (mut r, _) = leader(3, 3, &[1, 2]);
    let out = step(&mut r, ack(2, 3, true, 2, 0));
    assert!(
        applied(&out).is_empty(),
        "index 2 is from term 2 (Figure 8)"
    );
    assert_eq!(r.status().commit, 0);
    let out = step(&mut r, ack(2, 3, true, 3, 0));
    assert_eq!(applied(&out), vec![1, 2, 3]);
}

#[test]
fn commit_needs_a_majority_of_five() {
    let (mut r, _) = leader(5, 1, &[]);
    assert!(applied(&step(&mut r, ack(2, 1, true, 1, 0))).is_empty());
    assert!(
        applied(&step(&mut r, ack(2, 1, true, 1, 0))).is_empty(),
        "duplicate ack"
    );
    assert_eq!(applied(&step(&mut r, ack(4, 1, true, 1, 0))), vec![1]);
}

#[test]
fn apply_is_contiguous_and_never_repeated() {
    let (mut r, _) = leader(3, 1, &[]);
    let mut all = applied(&step(&mut r, ack(2, 1, true, 1, 0)));
    for tag in 0..5 {
        step(&mut r, Input::Propose { tag, data: vec![] });
        all.extend(applied(&step(&mut r, ack(3, 1, true, tag + 2, 0))));
        all.extend(applied(&step(&mut r, ack(2, 1, true, tag + 2, 0))));
        all.extend(applied(&step(&mut r, ack(3, 1, true, tag + 2, 0))));
    }
    assert_eq!(all, (1..=6).collect::<Vec<_>>());
}

#[test]
fn reject_backs_off_using_hint() {
    let (mut r, _) = leader(3, 3, &[1; 10]);
    let out = step(&mut r, ack(2, 3, false, 0, 5));
    assert_eq!(r.progress[&2].next, 5);
    let a = appends(&out);
    assert_eq!((a[0].0, a[0].1, a[0].2), (2, 4, 1));
    assert_eq!(a[0].3.len(), 7);

    step(&mut r, ack(2, 3, false, 0, 100));
    assert_eq!(
        r.progress[&2].next, 4,
        "a hint past next falls back to next - 1"
    );
}

#[test]
fn reject_never_backs_off_below_match() {
    let (mut r, _) = leader(3, 3, &[1; 10]);
    step(&mut r, ack(2, 3, true, 8, 0));
    let out = step(&mut r, ack(2, 3, false, 0, 1));
    assert_eq!(
        r.progress[&2],
        Progress {
            next: 9,
            matched: 8
        }
    );
    assert_eq!(appends(&out)[0].1, 8);
    assert!(
        step(&mut r, ack(2, 3, false, 0, 1)).is_empty(),
        "stale reject is a no-op"
    );
}

#[test]
fn stale_success_does_not_regress_progress() {
    let (mut r, _) = leader(3, 2, &[1; 4]);
    step(&mut r, ack(2, 2, true, 5, 0));
    step(&mut r, ack(2, 2, true, 3, 0));
    assert_eq!(
        r.progress[&2],
        Progress {
            next: 6,
            matched: 5
        }
    );
}

#[test]
fn batches_are_capped_and_continue_on_ack() {
    let mut r = node(1, 3, 2, &[1; 10]);
    r.cfg.max_append_entries = 3;
    r.become_leader();
    let out = drain(&mut r);
    assert_eq!(appends(&out)[0].3, vec![noop(2, 11)]);

    let out = step(&mut r, ack(2, 2, false, 0, 1));
    assert_eq!(appends(&out)[0].3, log(&[1; 3]));
    let out = step(&mut r, ack(2, 2, true, 3, 0));
    let a = appends(&out);
    assert_eq!((a[0].1, a[0].3.len()), (3, 3));
    assert_eq!(a[0].3[0].index, 4);
}

#[test]
fn higher_term_response_steps_down() {
    let (mut r, _) = leader(3, 2, &[]);
    let out = step(&mut r, ack(3, 5, false, 0, 1));
    assert_eq!(
        out,
        vec![Output::PersistHardState(HardState {
            term: 5,
            voted_for: None
        })]
    );
    let s = r.status();
    assert_eq!((s.role, s.term, s.leader), (Role::Follower, 5, None));
    assert!(r.progress.is_empty());
}

#[test]
fn stale_or_misaddressed_responses_are_ignored() {
    let (mut r, _) = leader(3, 4, &[1]);
    let before = r.clone();
    let cases = [
        ack(2, 3, true, 2, 0),
        ack(9, 4, true, 2, 0),
        ack(1, 4, true, 2, 0),
        ack(2, 4, true, 99, 0),
        Input::Receive(Envelope {
            from: 2,
            to: 3,
            msg: Message::AppendEntriesResp {
                term: 4,
                success: true,
                match_index: 2,
                hint: 0,
            },
        }),
    ];
    for input in cases {
        assert!(step(&mut r, input.clone()).is_empty(), "{input:?}");
    }
    assert_eq!(r.progress, before.progress);
    assert_eq!(r.commit, before.commit);
}

#[test]
fn followers_ignore_append_responses() {
    let mut r = node(1, 3, 2, &[2]);
    assert!(step(&mut r, ack(2, 2, true, 1, 0)).is_empty());
    assert_eq!(r.status().commit, 0);
}

#[test]
fn malformed_requests_never_reach_handlers() {
    let mut r = node(1, 3, 2, &[1]);
    let ae = |leader, prev_log_index, prev_log_term, entries| {
        Input::Receive(Envelope {
            from: 2,
            to: 1,
            msg: Message::AppendEntries {
                term: 2,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: 0,
            },
        })
    };
    let at = |term, index| Entry {
        term,
        index,
        payload: Payload::Noop,
    };
    let cases = [
        ae(3, 1, 1, vec![]),
        ae(2, 1, 3, vec![]),
        ae(2, 1, 1, vec![at(2, 3)]),
        ae(2, 1, 1, vec![at(2, 2), at(2, 2)]),
        ae(2, 1, 1, vec![at(3, 2)]),
        ae(2, 1, 2, vec![at(1, 2)]),
        ae(2, u64::MAX, 1, vec![at(2, 0)]),
        Input::Receive(Envelope {
            from: 2,
            to: 1,
            msg: Message::RequestVote {
                term: 3,
                candidate: 3,
                last_log_index: 1,
                last_log_term: 1,
            },
        }),
    ];
    for input in cases {
        assert!(step(&mut r, input.clone()).is_empty(), "{input:?}");
    }
    assert_eq!(r.status().term, 2);
}

#[test]
fn helpers_maintain_log_and_emit_outputs() {
    let mut r = node(1, 3, 2, &[1, 1, 2]);
    assert_eq!((r.last_log_index(), r.last_log_term()), (3, 2));
    assert_eq!(
        (r.term_at(0), r.term_at(1), r.term_at(3), r.term_at(4)),
        (Some(0), Some(1), Some(2), None)
    );

    r.truncate_local(5);
    r.truncate_local(2);
    r.append_local(vec![]);
    r.append_local(vec![noop(2, 2)]);
    r.send(
        3,
        Message::RequestVoteResp {
            term: 2,
            granted: true,
        },
    );
    r.persist_hard_state();
    assert_eq!(r.log(), &[log(&[1])[0].clone(), noop(2, 2)]);
    assert_eq!(
        drain(&mut r),
        vec![
            Output::Truncate { from: 2 },
            Output::Append(vec![noop(2, 2)]),
            Output::Send(Envelope {
                from: 1,
                to: 3,
                msg: Message::RequestVoteResp {
                    term: 2,
                    granted: true
                }
            }),
            Output::PersistHardState(HardState {
                term: 2,
                voted_for: None
            }),
        ]
    );
}

#[test]
fn commit_to_is_monotone_and_capped() {
    let mut r = node(2, 3, 1, &[1, 1, 1]);
    r.commit_to(2);
    r.commit_to(1);
    r.commit_to(10);
    r.commit_to(3);
    let applied: Vec<Vec<LogIndex>> = drain(&mut r)
        .into_iter()
        .map(|o| match o {
            Output::Apply(es) => es.iter().map(|e| e.index).collect(),
            o => panic!("unexpected {o:?}"),
        })
        .collect();
    assert_eq!(applied, vec![vec![1, 2], vec![3]]);
    assert_eq!(r.status().commit, 3);
}

#[test]
#[should_panic(expected = "truncating committed entry")]
fn truncating_committed_entries_is_a_bug() {
    let mut r = node(2, 3, 1, &[1, 1]);
    r.commit_to(2);
    r.truncate_local(2);
}

#[test]
fn step_orders_storage_before_sends() {
    let mut r = node(1, 3, 2, &[1]);
    r.role = Role::Leader;
    r.send(
        2,
        Message::RequestVoteResp {
            term: 2,
            granted: false,
        },
    );
    r.persist_hard_state();
    let out = step(
        &mut r,
        Input::Propose {
            tag: 0,
            data: vec![],
        },
    );
    let first_non_storage = out
        .iter()
        .position(|o| !super::core::is_storage(o))
        .unwrap();
    assert!(
        out[first_non_storage..]
            .iter()
            .all(|o| !super::core::is_storage(o))
    );
    assert_eq!(first_non_storage, 2);
}

/// Random acks, rejects, proposals and ticks against a leader: never panics, storage outputs
/// precede sends, `Apply` is contiguous, and progress stays within the log.
#[test]
fn leader_fuzz() {
    let mut rng = 0x5eed_u64;
    let mut next = |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    for _ in 0..200 {
        let (mut r, _) = leader(5, 3, &[1, 2, 2]);
        let mut applied_upto = 0;
        for _ in 0..300 {
            let input = match next(4) {
                0 => Input::Tick,
                1 => Input::Propose {
                    tag: 0,
                    data: vec![],
                },
                _ => ack(
                    1 + next(6),
                    2 + next(2),
                    next(2) == 0,
                    next(r.last_log_index() + 3),
                    next(r.last_log_index() + 3),
                ),
            };
            let out = step(&mut r, input);
            let split = out
                .iter()
                .take_while(|o| super::core::is_storage(o))
                .count();
            assert!(out[split..].iter().all(|o| !super::core::is_storage(o)));
            for i in applied(&out) {
                applied_upto += 1;
                assert_eq!(i, applied_upto);
            }
            assert_eq!(r.status().commit, applied_upto);
            if r.status().role != Role::Leader {
                break;
            }
            for pr in r.progress.values() {
                assert!(pr.matched < pr.next && pr.next <= r.last_log_index() + 1);
            }
        }
    }
}
