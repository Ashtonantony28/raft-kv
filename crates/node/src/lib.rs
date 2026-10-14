use kv::{ClientRequest, KvState};
use raft::{Config, Envelope, NodeId, Raft, Status};
use sys::{Disk, LogStore, Nanos, Result, Transport};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Tick,
    Message(Envelope),
    Client(ClientRequest),
}

pub struct Node<D: Disk, T: Transport> {
    raft: Raft,
    store: LogStore<D>,
    kv: KvState,
    transport: T,
}

impl<D: Disk, T: Transport> Node<D, T> {
    pub fn start(id: NodeId, cfg: Config, disk: D, transport: T, entropy: u64) -> Result<Self> {
        let _ = (id, cfg, disk, transport, entropy);
        todo!("SG6")
    }

    pub fn handle(&mut self, now: Nanos, ev: Event) -> Result<()> {
        let _ = (&self.raft, &self.store, &self.transport, now, ev);
        todo!("SG6")
    }

    pub fn status(&self) -> Status {
        self.raft.status()
    }

    pub fn kv(&self) -> &KvState {
        &self.kv
    }

    pub fn into_disk(self) -> D {
        self.store.into_disk()
    }
}
