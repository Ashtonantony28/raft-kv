use raft::{Output, Restored};

use crate::{Disk, Result};

pub struct LogStore<D: Disk> {
    disk: D,
}

impl<D: Disk> LogStore<D> {
    pub fn open(disk: D) -> Result<(Self, Restored)> {
        let _ = disk;
        todo!("SG3")
    }

    pub fn apply(&mut self, out: &Output) -> Result<()> {
        let _ = (&self.disk, out);
        todo!("SG3")
    }

    pub fn sync(&mut self) -> Result<()> {
        todo!("SG3")
    }

    pub fn disk(&self) -> &D {
        &self.disk
    }

    pub fn into_disk(self) -> D {
        self.disk
    }
}
