//! SG2-H: leader election, hand-written by Ashton. Tests: `tests/election.rs`.

use crate::{LogIndex, NodeId, Raft, Term};

impl Raft {
    /// Election timer fired as follower or candidate: become candidate, bump the term, vote for
    /// self, persist, reset the timer, and send `RequestVote` to every other peer. A single-node
    /// cluster wins immediately.
    pub(crate) fn on_election_timeout(&mut self) {
        todo!("SG2-H: on_election_timeout")
    }

    /// Figure 2 RequestVote receiver: step down on a higher term, grant if `voted_for` is empty
    /// or `candidate` and the candidate's log is at least as up-to-date (§5.4.1). Persist the
    /// vote and reset the election timer when granting; always reply with the current term.
    pub(crate) fn handle_request_vote(
        &mut self,
        candidate: NodeId,
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) {
        let _ = (candidate, term, last_log_index, last_log_term);
        todo!("SG2-H: handle_request_vote")
    }

    /// Count a vote. Step down on a higher term, ignore stale terms and non-candidates, count
    /// each voter once, and `become_leader` on a quorum.
    pub(crate) fn handle_request_vote_resp(&mut self, from: NodeId, term: Term, granted: bool) {
        let _ = (from, term, granted);
        todo!("SG2-H: handle_request_vote_resp")
    }
}
