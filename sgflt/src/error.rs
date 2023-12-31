use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub enum SgfitErr {
    ChunkFull(usize),
}

impl SgfitErr {
    pub fn new_chunk_full(cap: usize) -> Self {
        SgfitErr::ChunkFull(cap)
    }
}

impl Debug for SgfitErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Error for SgfitErr {}

impl Display for SgfitErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SgfitErr::ChunkFull(cap) => {
                write!(f, "SgfitErr::ChunkFull[cap:{}]", cap)
            }
        }
    }
}
