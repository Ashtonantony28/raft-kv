//! SG2-H leader election suite. Run with `cargo test -p raft --test election -- --include-ignored`
//! and delete the `#[ignore]`s once green.

mod common;

use std::collections::BTreeSet;

use common::*;
use raft::{Config, Message, Output, Raft, Restored, Role};

/// Node 1 of `n`, elected leader of term 2 with log `[1]` plus its term-2 noop.
fn make_leader(n: u64) -> Raft {
    let mut r = node(1, n, hs(1, None), &[1]);
    tick_until_send(&mut r, 20);
    for from in 2..=Config::new(1, (1..=n).collect()).quorum() as u64 {
        step(&mut r, vote(from, 1, 2, true));
    }
    assert_eq!(r.status().role, Role::Leader);
    r
}

#[test]
#[ignore = "SG2-H"]
fn election_timeout_is_randomized_within_range() {
    let mut seen = BTreeSet::new();
    for entropy in 0..50 {
        let mut r = Raft::new(Config::new(1, vec![1, 2, 3]), Restored::default(), entropy);
        let (ticks, _) = tick_until_send(&mut r, 20);
        assert!((10..20).contains(&ticks), "timed out after {ticks} ticks");
        seen.insert(ticks);
    }
    assert!(seen.len() > 3, "timeouts are not randomized: {seen:?}");
}

#[test]
#[ignore = "SG2-H"]
fn timeout_starts_an_election() {
    let mut r = node(1, 3, hs(3, None), &[1, 1, 2]);
    step(&mut r, append(2, 1, 3, (3, 2), vec![], 0));
    assert_eq!(r.status().leader, Some(2));
    let (_, out) = tick_until_send(&mut r, 20);
    assert_eq!(persisted(&out).last(), Some(&hs(4, Some(1))));
    let mut to = Vec::new();
    for env in sent(&out) {
        let expected = Message::RequestVote {
            term: 4,
            candidate: 1,
            last_log_index: 3,
            last_log_term: 2,
        };
        assert_eq!(env.msg, expected);
        to.push(env.to);
    }
    to.sort();
    assert_eq!(to, vec![2, 3]);
    let s = r.status();
    assert_eq!((s.role, s.term, s.leader), (Role::Candidate, 4, None));
}

#[test]
#[ignore = "SG2-H"]
fn single_node_elects_itself() {
    let mut r = Raft::new(Config::new(1, vec![1]), Restored::default(), 1);
    let mut out = Vec::new();
    for _ in 0..20 {
        out.extend(step(&mut r, raft::Input::Tick));
    }
    let s = r.status();
    assert_eq!(
        (s.role, s.term, s.leader, s.commit),
        (Role::Leader, 1, Some(1), 1)
    );
    assert_eq!(applied(&out), vec![noop(1, 1)]);
}

#[test]
#[ignore = "SG2-H"]
fn grants_vote_and_persists_it_before_replying() {
    let mut r = node(1, 3, hs(2, None), &[1, 2]);
    let out = step(&mut r, request_vote(2, 1, 3, 2, 2));
    assert_eq!(persisted(&out).last(), Some(&hs(3, Some(2))));
    assert_eq!(vote_resp(&out), (3, true));
    let s = r.status();
    assert_eq!((s.role, s.term), (Role::Follower, 3));
}

#[test]
#[ignore = "SG2-H"]
fn rejects_candidate_from_stale_term() {
    let mut r = node(1, 3, hs(5, None), &[1]);
    let out = step(&mut r, request_vote(2, 1, 4, 9, 4));
    assert_eq!(vote_resp(&out), (5, false));
    assert!(persisted(&out).is_empty());
    assert_eq!(r.status().term, 5);
}

#[test]
#[ignore = "SG2-H"]
fn votes_once_per_term() {
    let mut r = node(1, 3, hs(2, None), &[1]);
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(2, 1, 3, 1, 1))),
        (3, true)
    );
    let out = step(&mut r, request_vote(3, 1, 3, 1, 1));
    assert_eq!(vote_resp(&out), (3, false));
    assert!(persisted(&out).is_empty());
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(2, 1, 3, 1, 1))),
        (3, true),
        "re-delivered request from the same candidate"
    );
}

#[test]
#[ignore = "SG2-H"]
fn restored_vote_is_respected() {
    let mut r = node(1, 3, hs(3, Some(2)), &[1]);
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(3, 1, 3, 1, 1))),
        (3, false)
    );
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(2, 1, 3, 1, 1))),
        (3, true)
    );
}

#[test]
#[ignore = "SG2-H"]
fn vote_survives_a_crash() {
    let mut c = Cluster::fresh(3);
    assert_eq!(
        vote_resp(&c.step(1, request_vote(2, 1, 1, 0, 0))),
        (1, true)
    );
    c.restart(1, 99);
    assert_eq!(
        vote_resp(&c.step(1, request_vote(3, 1, 1, 0, 0))),
        (1, false)
    );
}

#[test]
#[ignore = "SG2-H"]
fn grants_only_to_up_to_date_candidates() {
    // Voter log has last term 2 at index 3 (§5.4.1).
    let cases = [
        ((5, 1), false),
        ((2, 2), false),
        ((3, 2), true),
        ((4, 2), true),
        ((1, 3), true),
    ];
    for ((last_index, last_term), granted) in cases {
        let mut r = node(1, 3, hs(2, None), &[1, 1, 2]);
        let out = step(&mut r, request_vote(2, 1, 3, last_index, last_term));
        assert_eq!(
            vote_resp(&out),
            (3, granted),
            "candidate last ({last_index}, {last_term})"
        );
    }
}

#[test]
#[ignore = "SG2-H"]
fn rejected_higher_term_candidate_still_bumps_term() {
    let mut r = node(1, 3, hs(2, Some(1)), &[1, 2]);
    let out = step(&mut r, request_vote(2, 1, 5, 1, 1));
    assert_eq!(vote_resp(&out), (5, false));
    assert_eq!(persisted(&out).last(), Some(&hs(5, None)));
    let s = r.status();
    assert_eq!((s.role, s.term), (Role::Follower, 5));
}

#[test]
#[ignore = "SG2-H"]
fn granting_a_vote_resets_the_election_timer() {
    let mut r = node(1, 3, hs(2, None), &[]);
    for _ in 0..9 {
        assert!(step(&mut r, raft::Input::Tick).is_empty());
    }
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(2, 1, 2, 0, 0))),
        (2, true)
    );
    for _ in 0..9 {
        assert!(sent(&step(&mut r, raft::Input::Tick)).is_empty());
    }
}

#[test]
#[ignore = "SG2-H"]
fn candidate_with_majority_becomes_leader() {
    let mut r = node(1, 3, hs(1, None), &[1]);
    tick_until_send(&mut r, 20);
    let out = step(&mut r, vote(2, 1, 2, true));
    let s = r.status();
    assert_eq!((s.role, s.term, s.leader), (Role::Leader, 2, Some(1)));
    assert!(out.contains(&Output::Append(vec![noop(2, 2)])));
    let mut to = Vec::new();
    for env in sent(&out) {
        if let Message::AppendEntries {
            term,
            prev_log_index,
            prev_log_term,
            entries,
            ..
        } = env.msg
        {
            assert_eq!((term, prev_log_index, prev_log_term), (2, 1, 1));
            assert_eq!(entries, vec![noop(2, 2)]);
            to.push(env.to);
        }
    }
    to.sort();
    assert_eq!(to, vec![2, 3]);
}

#[test]
#[ignore = "SG2-H"]
fn each_voter_counts_once() {
    let mut r = node(1, 5, hs(1, None), &[]);
    tick_until_send(&mut r, 20);
    step(&mut r, vote(2, 1, 2, true));
    step(&mut r, vote(2, 1, 2, true));
    step(&mut r, vote(3, 1, 2, false));
    step(&mut r, vote(4, 1, 2, false));
    assert_eq!(r.status().role, Role::Candidate);
    step(&mut r, vote(5, 1, 2, true));
    assert_eq!(r.status().role, Role::Leader);
}

#[test]
#[ignore = "SG2-H"]
fn split_vote_leads_to_a_new_election() {
    let mut r = node(1, 3, hs(1, None), &[]);
    tick_until_send(&mut r, 20);
    step(&mut r, vote(2, 1, 2, false));
    step(&mut r, vote(3, 1, 2, false));
    assert_eq!(r.status().role, Role::Candidate);

    let (ticks, out) = tick_until_send(&mut r, 20);
    assert!((10..20).contains(&ticks), "re-election after {ticks} ticks");
    assert_eq!(persisted(&out).last(), Some(&hs(3, Some(1))));
    assert_eq!(r.status().term, 3);

    step(&mut r, vote(2, 1, 2, true));
    assert_eq!(
        r.status().role,
        Role::Candidate,
        "a vote from term 2 counts for nothing"
    );
    step(&mut r, vote(2, 1, 3, true));
    assert_eq!(r.status().role, Role::Leader);
}

#[test]
#[ignore = "SG2-H"]
fn votes_from_an_earlier_election_do_not_carry_over() {
    let mut r = node(1, 5, hs(1, None), &[]);
    tick_until_send(&mut r, 20);
    step(&mut r, vote(2, 1, 2, true));
    tick_until_send(&mut r, 20);
    assert_eq!(r.status().term, 3);
    step(&mut r, vote(3, 1, 3, true));
    assert_eq!(
        r.status().role,
        Role::Candidate,
        "node 2's term-2 vote was counted in term 3"
    );
    step(&mut r, vote(4, 1, 3, true));
    assert_eq!(r.status().role, Role::Leader);
}

#[test]
#[ignore = "SG2-H"]
fn rejected_vote_does_not_reset_the_election_timer() {
    let mut r = node(1, 3, hs(2, None), &[1, 2]);
    for _ in 0..9 {
        assert!(step(&mut r, raft::Input::Tick).is_empty());
    }
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(2, 1, 2, 1, 1))),
        (2, false)
    );
    let (ticks, _) = tick_until_send(&mut r, 20);
    assert!(ticks <= 10, "timer was reset by a rejected vote");
}

#[test]
#[ignore = "SG2-H"]
fn candidate_yields_to_leader_of_same_term() {
    let mut r = node(1, 3, hs(1, None), &[1]);
    tick_until_send(&mut r, 20);
    let (term, success, match_index, _) =
        append_resp(&step(&mut r, append(2, 1, 2, (1, 1), vec![], 0)));
    assert_eq!((term, success, match_index), (2, true, 1));
    let s = r.status();
    assert_eq!((s.role, s.term, s.leader), (Role::Follower, 2, Some(2)));
}

#[test]
#[ignore = "SG2-H"]
fn candidate_steps_down_on_higher_term_vote_response() {
    let mut r = node(1, 3, hs(1, None), &[]);
    tick_until_send(&mut r, 20);
    let out = step(&mut r, vote(2, 1, 7, false));
    assert_eq!(persisted(&out).last(), Some(&hs(7, None)));
    let s = r.status();
    assert_eq!((s.role, s.term), (Role::Follower, 7));
}

#[test]
#[ignore = "SG2-H"]
fn leader_steps_down_on_higher_term() {
    let mut r = make_leader(3);
    let out = step(&mut r, request_vote(3, 1, 5, 2, 2));
    assert_eq!(vote_resp(&out), (5, true));
    assert_eq!(persisted(&out).last(), Some(&hs(5, Some(3))));
    let s = r.status();
    assert_eq!((s.role, s.term, s.leader), (Role::Follower, 5, None));

    let mut r = make_leader(3);
    step(&mut r, append(3, 1, 5, (2, 2), vec![], 0));
    let s = r.status();
    assert_eq!((s.role, s.term, s.leader), (Role::Follower, 5, Some(3)));

    let mut r = make_leader(3);
    step(&mut r, vote(2, 1, 5, false));
    let s = r.status();
    assert_eq!((s.role, s.term), (Role::Follower, 5));
}

#[test]
#[ignore = "SG2-H"]
fn leader_rejects_candidates_of_its_own_term() {
    let mut r = make_leader(3);
    assert_eq!(
        vote_resp(&step(&mut r, request_vote(3, 1, 2, 9, 2))),
        (2, false)
    );
    assert_eq!(r.status().role, Role::Leader);
}

#[test]
#[ignore = "SG2-H"]
fn leader_never_starts_an_election() {
    let mut r = make_leader(3);
    for _ in 0..200 {
        for env in sent(&step(&mut r, raft::Input::Tick)) {
            assert!(matches!(env.msg, Message::AppendEntries { .. }), "{env:?}");
        }
    }
    let s = r.status();
    assert_eq!((s.role, s.term), (Role::Leader, 2));
}

#[test]
#[ignore = "SG2-H"]
fn cluster_elects_one_leader_everyone_follows() {
    let mut c = Cluster::fresh(5);
    c.elect(3);
    c.heartbeat(3);
    for id in 1..=5 {
        let s = c.raft(id).status();
        assert_eq!((s.term, s.leader, s.commit), (1, Some(3), 1), "node {id}");
        assert_eq!(c.log(id), vec![noop(1, 1)]);
    }
}

#[test]
#[ignore = "SG2-H"]
fn randomized_timeouts_converge_on_a_leader() {
    for seed in 0..20 {
        let mut c = Cluster::fresh(5);
        for id in 1..=5 {
            c.restart(id, seed * 31 + id);
        }
        let mut rounds = 0;
        while !(1..=5).any(|id| c.role(id) == Role::Leader) {
            rounds += 1;
            assert!(rounds < 1000, "seed {seed}: no leader");
            for id in 1..=5 {
                c.tick(id);
            }
            c.deliver_all();
        }
    }
}
