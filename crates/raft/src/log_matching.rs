//! SG2-H: follower side of log replication, hand-written by Ashton. Tests: `tests/log_matching.rs`.
//!
//! `entries` are already checked to be contiguous after `prev_log_index` with terms no newer
//! than `term`, and `leader` is the sender.

use crate::{Entry, LogIndex, NodeId, Raft, Term};

impl Raft {
    /// Figure 2 AppendEntries receiver:
    /// - reject a stale term; otherwise adopt the term and recognise `leader` (`become_follower`);
    /// - reject if the log has no entry at `prev_log_index` with `prev_log_term`, with a `hint`
    ///   for the leader's next probe (D-08);
    /// - truncate only from the first conflicting entry, append what is new, never re-append
    ///   entries already present;
    /// - commit up to `min(leader_commit, index of last new entry)` via `commit_to`;
    /// - reply with `match_index = prev_log_index + entries.len()` on success.
    pub(crate) fn handle_append_entries(
        &mut self,
        leader: NodeId,
        term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    ) {
        let _ = (
            leader,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        );
        todo!("SG2-H: handle_append_entries")
    }
}
