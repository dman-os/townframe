//! `wasi:random/random`: the secure entropy source. A guest's request and span ids come from it
//! (`big_ndk`'s getrandom `custom` backend calls `get-random-bytes`), so every micro that links
//! `clay_mdk` imports it. Backed by the platform's CSPRNG through `getrandom` (the OS natively,
//! `crypto.getRandomValues` in the browser), unlike `insecure-seed` which is only wasi-libc's
//! hashing jitter.

use wcmp::Error;
use wcmp::Linker;

use super::identifier;

const RANDOM: &str = "wasi:random/random@0.3.0";

/// The most bytes one `get-random-bytes` call returns. The length is the guest's to choose and the
/// host allocates it, so it is bounded: a guest asking for more faults (its call fails), and the
/// host never sizes an allocation from untrusted input. Identifiers and seeds take tens of bytes.
const MAX_RANDOM_BYTES: u64 = 1 << 20;

pub(super) fn define_random<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    let mut random = linker.instance(&identifier(RANDOM));
    random.func_wrap("get-random-bytes", |_, (len,): (u64,)| {
        if len > MAX_RANDOM_BYTES {
            return Err(Error::Internal {
                message: format!(
                    "`get-random-bytes` asked for {len} bytes; one call returns at most {MAX_RANDOM_BYTES}"
                ),
            });
        }
        let len = usize::try_from(len).expect("the bound fits a usize on every target");
        let mut bytes = vec![0_u8; len];
        getrandom::fill(&mut bytes).expect("the platform has entropy");
        Ok((bytes,))
    })?;
    random.func_wrap("get-random-u64", |_, (): ()| {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes).expect("the platform has entropy");
        Ok((u64::from_le_bytes(bytes),))
    })
}
