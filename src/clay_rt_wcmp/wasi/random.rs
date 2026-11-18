//! `wasi:random/random`: the secure entropy source. A guest's request and span ids come from it
//! (`big_ndk`'s getrandom `custom` backend calls `get-random-bytes`), so every micro that links
//! `clay_mdk` imports it. Backed by the platform's CSPRNG through `getrandom` (the OS natively,
//! `crypto.getRandomValues` in the browser), unlike `insecure-seed` which is only wasi-libc's
//! hashing jitter.

use wcmp::Error;
use wcmp::Linker;

use super::identifier;

const RANDOM: &str = "wasi:random/random@0.3.0";

pub(super) fn define_random<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    let mut random = linker.instance(&identifier(RANDOM));
    random.func_wrap("get-random-bytes", |_, (len,): (u64,)| {
        let mut bytes = vec![0_u8; usize::try_from(len).expect("a guest asks for what fits memory")];
        getrandom::fill(&mut bytes).expect("the platform has entropy");
        Ok((bytes,))
    })?;
    random.func_wrap("get-random-u64", |_, (): ()| {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes).expect("the platform has entropy");
        Ok((u64::from_le_bytes(bytes),))
    })
}
