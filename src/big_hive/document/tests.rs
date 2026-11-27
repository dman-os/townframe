//! Tests of [`Document`]. The laws are in the [parent module](super); each law test below is
//! named after its number and checks it against the naive [`oracle::World`]. The whole-machine
//! simulation, which is where the laws meet crashes and disorder, is [`super::sim`].

use super::*;

mod invariants;
mod laws;
pub(super) mod oracle;
