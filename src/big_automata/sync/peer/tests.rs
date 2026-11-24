//! Law tests of [`PeerSync`] over the scripted [`rig::Rig`]: hand-built answers, one call at a
//! time, every flush checked against law 13's recomputation. The laws are in the
//! [parent module](crate::sync); the whole-machine simulation is a separate suite.

use super::*;

use crate::replay::wire::Verdict;
use crate::sync::bucket::PartObjDelta;

mod calls;
mod facts;
mod hostile;
mod rig;
mod routing;
mod walks;

use rig::*;
