//! SG2-H log matching suite (follower side of AppendEntries). Run with
//! `cargo test -p raft --test log_matching -- --include-ignored` and delete the `#[ignore]`s
//! once green. Node 1 is the follower under test and node 2 the leader unless stated.

mod common;

use common::*;
use raft::{Output, Role, Term};

fn follower(term: Term, terms: &[Term]) -> raft::Raft {
    node(1, 3, hs(term, None), terms)
}

#[test]
#[ignore = "SG2-H"]
fn heartbeat_from_current_leader_succeeds() {
    let mut r = follower(2, &[1, 2]);
    let out = step(&mut r, append(2, 1, 2, (2, 2), vec![], 0));
    let (term, success, match_index, _) = append_resp(&out);
    assert_eq!((term, success, match_index), (2, true, 2));
    assert!(!has_storage_write(&out));
    let s = r.status();
    assert_eq!((s.role, s.leader), (Role::Follower, Some(2)));
}

#[test]
#[ignore = "SG2-H"]
fn rejects_stale_term_without_side_effects() {
    let mut r = follower(3, &[1, 2]);
    let out = step(&mut r, append(2, 1, 2, (2, 2), vec![entry(2, 3)], 3));
    let (term, success, _, _) = append_resp(&out);
    assert_eq!((term, success), (3, false));
    assert!(!has_storage_write(&out) && persisted(&out).is_empty() && applied(&out).is_empty());
    assert_eq!(r.log(), entries(&[1, 2]).as_slice());
    assert_eq!(r.status().leader, None);
}

#[test]
#[ignore = "SG2-H"]
fn adopts_higher_term() {
    let mut r = follower(1, &[1]);
    let out = step(&mut r, append(2, 1, 3, (1, 1), vec![], 0));
    assert_eq!(persisted(&out).last(), Some(&hs(3, None)));
    assert!(append_resp(&out).1);
    let s = r.status();
    assert_eq!((s.term, s.leader), (3, Some(2)));
}

#[test]
#[ignore = "SG2-H"]
fn rejects_when_prev_index_is_missing() {
    let mut r = follower(2, &[1, 1]);
    let out = step(&mut r, append(2, 1, 2, (5, 2), vec![entry(2, 6)], 0));
    let (term, success, _, hint) = append_resp(&out);
    assert_eq!((term, success), (2, false));
    assert!(
        (1..=3).contains(&hint),
        "hint {hint} must be a next index within the log + 1"
    );
    assert!(!has_storage_write(&out));
    assert_eq!(
        r.status().leader,
        Some(2),
        "a reject still recognises the leader"
    );
}

#[test]
#[ignore = "SG2-H"]
fn rejects_when_prev_term_differs() {
    let mut r = follower(3, &[1, 1, 2, 2]);
    let out = step(&mut r, append(2, 1, 3, (4, 3), vec![entry(3, 5)], 0));
    let (_, success, _, hint) = append_resp(&out);
    assert!(!success);
    assert!(
        (1..=4).contains(&hint),
        "hint {hint} must probe at or before the conflict"
    );
    assert!(
        !has_storage_write(&out),
        "a rejected request never modifies the log"
    );
    assert_eq!(r.log(), entries(&[1, 1, 2, 2]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn appends_new_entries() {
    let mut r = follower(2, &[1]);
    let new = vec![entry(2, 2), entry(2, 3)];
    let out = step(&mut r, append(2, 1, 2, (1, 1), new.clone(), 0));
    assert!(out.contains(&Output::Append(new)));
    assert!(!out.iter().any(|o| matches!(o, Output::Truncate { .. })));
    assert_eq!(append_resp(&out).2, 3);
    assert_eq!(r.log(), entries(&[1, 2, 2]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn truncates_conflicting_suffix() {
    let mut r = follower(1, &[1, 1, 1, 1]);
    let out = step(&mut r, append(2, 1, 2, (2, 1), vec![entry(2, 3)], 0));
    let truncate = out.iter().position(|o| *o == Output::Truncate { from: 3 });
    let append = out
        .iter()
        .position(|o| *o == Output::Append(vec![entry(2, 3)]));
    assert!(truncate.is_some() && truncate < append, "{out:?}");
    let (_, success, match_index, _) = append_resp(&out);
    assert_eq!((success, match_index), (true, 3));
    assert_eq!(r.log(), entries(&[1, 1, 2]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn truncates_only_from_first_conflict() {
    let mut r = follower(2, &[1, 1, 1, 1]);
    let new = vec![entry(1, 2), entry(1, 3), entry(2, 4), entry(2, 5)];
    let out = step(&mut r, append(2, 1, 2, (1, 1), new, 0));
    assert!(out.contains(&Output::Truncate { from: 4 }), "{out:?}");
    assert!(out.contains(&Output::Append(vec![entry(2, 4), entry(2, 5)])));
    assert_eq!(r.log(), entries(&[1, 1, 1, 2, 2]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn keeps_entries_after_a_matching_prefix() {
    let mut r = follower(2, &[1, 2, 2, 2]);
    let out = step(&mut r, append(2, 1, 2, (1, 1), vec![entry(2, 2)], 0));
    assert!(!has_storage_write(&out), "{out:?}");
    let (_, success, match_index, _) = append_resp(&out);
    assert_eq!((success, match_index), (true, 2));
    assert_eq!(r.log(), entries(&[1, 2, 2, 2]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn redelivery_is_idempotent() {
    let mut r = follower(1, &[]);
    let msg = append(2, 1, 1, (0, 0), vec![entry(1, 1), entry(1, 2)], 1);
    let first = step(&mut r, msg.clone());
    let second = step(&mut r, msg);
    assert!(!has_storage_write(&second), "{second:?}");
    assert!(applied(&second).is_empty());
    assert_eq!(append_resp(&first).2, append_resp(&second).2);
    assert_eq!(r.log(), entries(&[1, 1]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn reordered_appends_converge() {
    let mut r = follower(1, &[]);
    let early = append(2, 1, 1, (0, 0), vec![entry(1, 1), entry(1, 2)], 0);
    let late = append(2, 1, 1, (2, 1), vec![entry(1, 3)], 0);
    assert!(!append_resp(&step(&mut r, late.clone())).1);
    assert_eq!(append_resp(&step(&mut r, early.clone())).2, 2);
    assert_eq!(append_resp(&step(&mut r, late)).2, 3);
    let out = step(&mut r, early);
    assert!(
        !has_storage_write(&out),
        "a delayed shorter append must not truncate"
    );
    let (_, success, match_index, _) = append_resp(&out);
    assert_eq!((success, match_index), (true, 2));
    assert_eq!(r.log(), entries(&[1, 1, 1]).as_slice());
}

#[test]
#[ignore = "SG2-H"]
fn delayed_append_never_touches_committed_entries() {
    let mut r = follower(1, &[]);
    step(&mut r, append(2, 1, 1, (0, 0), entries(&[1, 1, 1]), 3));
    assert_eq!(r.status().commit, 3);
    let out = step(&mut r, append(2, 1, 1, (0, 0), entries(&[1]), 0));
    assert!(!has_storage_write(&out) && applied(&out).is_empty());
    assert_eq!(r.status().commit, 3);
    assert_eq!(r.log().len(), 3);
}

#[test]
#[ignore = "SG2-H"]
fn commit_is_capped_by_last_new_entry() {
    let mut r = follower(1, &[]);
    let out = step(&mut r, append(2, 1, 1, (0, 0), entries(&[1, 1]), 5));
    assert_eq!(applied(&out), entries(&[1, 1]));
    assert_eq!(r.status().commit, 2);
}

#[test]
#[ignore = "SG2-H"]
fn heartbeat_does_not_commit_an_unverified_suffix() {
    // Entries 2 and 3 have not been checked against the leader's log yet.
    let mut r = follower(1, &[1, 1, 1]);
    let out = step(&mut r, append(2, 1, 2, (1, 1), vec![], 3));
    assert_eq!(applied(&out), entries(&[1]));
    assert_eq!(r.status().commit, 1);
}

#[test]
#[ignore = "SG2-H"]
fn commit_never_decreases_and_applies_once() {
    let mut r = follower(1, &[]);
    let mut all = applied(&step(
        &mut r,
        append(2, 1, 1, (0, 0), entries(&[1, 1, 1]), 1),
    ));
    all.extend(applied(&step(&mut r, append(2, 1, 1, (3, 1), vec![], 3))));
    all.extend(applied(&step(&mut r, append(2, 1, 1, (3, 1), vec![], 2))));
    all.extend(applied(&step(&mut r, append(2, 1, 1, (3, 1), vec![], 3))));
    assert_eq!(all, entries(&[1, 1, 1]));
    assert_eq!(r.status().commit, 3);
}

#[test]
#[ignore = "SG2-H"]
fn heartbeats_keep_a_follower_from_timing_out() {
    let mut r = follower(1, &[]);
    for i in 0..200 {
        let out = if i % 5 == 0 {
            step(&mut r, append(2, 1, 1, (0, 0), vec![], 0))
        } else {
            step(&mut r, raft::Input::Tick)
        };
        assert_eq!(r.status().role, Role::Follower, "tick {i}: {out:?}");
    }
}

/// Figure 7: a term-8 leader brings each of the followers (a)–(f) in line with its log.
#[test]
#[ignore = "SG2-H"]
fn figure_7_followers_converge() {
    let leader: &[Term] = &[1, 1, 1, 4, 4, 5, 5, 6, 6, 6];
    let followers: [&[Term]; 6] = [
        &[1, 1, 1, 4, 4, 5, 5, 6, 6],
        &[1, 1, 1, 4],
        &[1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],
        &[1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7],
        &[1, 1, 1, 4, 4, 4, 4],
        &[1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3],
    ];
    for (case, follower) in ('a'..).zip(followers) {
        let mut c = Cluster::new(&[
            (hs(7, None), leader),
            (hs(7, None), leader),
            (hs(7, None), follower),
        ]);
        c.elect(1);
        assert_eq!(c.raft(1).status().term, 8);
        c.heartbeat(1);
        let mut expected = entries(leader);
        expected.push(noop(8, 11));
        for id in 1..=3 {
            assert_eq!(c.log(id), expected, "case ({case}), node {id}");
            assert_eq!(c.raft(id).status().commit, 11, "case ({case}), node {id}");
        }
    }
}

/// Figure 8 (d): entries replicated to a majority but never committed may be overwritten.
#[test]
#[ignore = "SG2-H"]
fn figure_8_uncommitted_entries_are_replaced() {
    let mut c = Cluster::new(&[
        (hs(4, Some(1)), &[1, 2, 4]),
        (hs(4, Some(1)), &[1, 2]),
        (hs(4, Some(1)), &[1, 2]),
        (hs(4, Some(1)), &[1]),
        (hs(4, None), &[1, 3]),
    ]);
    c.elect(5);
    c.heartbeat(5);
    let expected = vec![entry(1, 1), entry(3, 2), noop(5, 3)];
    for id in 1..=5 {
        assert_eq!(c.log(id), expected, "node {id}");
        assert_eq!(c.raft(id).status().commit, 3, "node {id}");
    }
}

/// Figure 8 (e): a leader commits an old-term entry only once an entry from its own term is on
/// a majority, after which a node missing it can never be elected.
#[test]
#[ignore = "SG2-H"]
fn figure_8_old_entries_commit_with_current_term_entry() {
    let mut c = Cluster::new(&[
        (hs(3, None), &[1, 2]),
        (hs(3, None), &[1, 2]),
        (hs(3, None), &[1, 2]),
        (hs(3, None), &[1]),
        (hs(3, None), &[1, 3]),
    ]);
    c.isolated.extend([4, 5]);
    while c.role(1) == Role::Follower {
        c.tick(1);
    }
    for _ in 0..4 {
        c.deliver_one();
    }
    assert_eq!(c.role(1), Role::Leader);
    c.net.clear();
    c.step(1, ack(2, 1, 4, 2));
    c.step(1, ack(3, 1, 4, 2));
    assert_eq!(c.raft(1).status().commit, 0, "index 2 is from term 2");

    c.heartbeat(1);
    assert_eq!(c.raft(1).status().commit, 3);
    assert_eq!(c.committed, vec![entry(1, 1), entry(2, 2), noop(4, 3)]);

    c.isolated.clear();
    for _ in 0..3 {
        let term = c.raft(5).status().term;
        while c.raft(5).status().term == term {
            c.tick(5);
        }
        c.deliver_all();
        assert_ne!(c.role(5), Role::Leader);
    }
    for id in 1..=3 {
        assert_eq!(
            c.log(id),
            vec![entry(1, 1), entry(2, 2), noop(4, 3)],
            "node {id}"
        );
    }
}
