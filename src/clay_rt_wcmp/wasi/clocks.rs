//! `wasi:clocks` and `wasi:random` on the polyfill linker, with a
//! browser-safe wall/monotonic clock (std time panics on
//! `wasm32-unknown-unknown`).

use std::sync::OnceLock;
use std::time::Duration;
#[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
use std::time::Instant;
#[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
use std::time::SystemTime;
#[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
use std::time::UNIX_EPOCH;

use wcmp::ComponentType;
use wcmp::Error;
use wcmp::Lift;
use wcmp::Linker;
use wcmp::Lower;

use super::identifier;

pub(super) const CLOCK_TYPES: &str = "wasi:clocks/types@0.3.0";
const MONOTONIC_CLOCK: &str = "wasi:clocks/monotonic-clock@0.3.0";
const SYSTEM_CLOCK: &str = "wasi:clocks/system-clock@0.3.0";
const RANDOM_INSECURE_SEED: &str = "wasi:random/insecure-seed@0.3.0";

pub(super) fn define_monotonic_clock<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    let mut clock = linker.instance(&identifier(MONOTONIC_CLOCK));
    clock.func_wrap("now", |_, (): ()| Ok((monotonic_nanos(),)))?;
    clock.func_wrap_concurrent(
        "wait-for",
        |_: &wcmp::Accessor<T>, (how_long,): (u64,)| async move {
            sleep(Duration::from_nanos(how_long)).await;
            Ok(())
        },
    )
}

pub(super) fn define_system_clock<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    linker
        .instance(&identifier(SYSTEM_CLOCK))
        .func_wrap("now", |_, (): ()| {
            let since_epoch = unix_time();
            Ok((SystemInstant {
                seconds: since_epoch.as_secs() as i64,
                nanoseconds: since_epoch.subsec_nanos(),
            },))
        })
}

pub(super) fn define_insecure_seed<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    linker
        .instance(&identifier(RANDOM_INSECURE_SEED))
        .func_wrap("get-insecure-seed", |_, (): ()| {
            let nanos = unix_time().as_nanos() as u64;
            // SplitMix64 over a wall-clock + counter mix: enough entropy
            // for wasi-libc's insecure uses (malloc jitter), never for
            // anything security-bearing.
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let mixed = nanos
                ^ COUNTER.fetch_add(0x9e37_79b9_7f4a_7c15, std::sync::atomic::Ordering::Relaxed);
            let a = splitmix64(mixed);
            let b = splitmix64(a ^ 0xbf58_476d_1ce4_e5b9);
            // One result: the WIT `tuple<u64, u64>` value.
            Ok(((a, b),))
        })
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Wall-clock duration since the unix epoch.
///
/// The browser host runs on `wasm32-unknown-unknown`, whose std time backend is
/// `unsupported` and panics on `SystemTime::now`; it reads `Date.now` instead.
/// Native and `wasm32-wasip3` hosts use `SystemTime`.
fn unix_time() -> Duration {
    #[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
    {
        let ms = js_sys::Date::now().max(0.0);
        Duration::from_secs_f64(ms / 1000.0)
    }
    #[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
    }
}

/// Nanoseconds since the first reading in this process.
fn monotonic_nanos() -> u64 {
    #[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
    {
        static ORIGIN_MS: OnceLock<f64> = OnceLock::new();
        let origin = *ORIGIN_MS.get_or_init(performance_ms);
        let elapsed_ms = (performance_ms() - origin).max(0.0);
        u64::try_from((elapsed_ms * 1_000_000.0) as u128).unwrap_or(u64::MAX)
    }
    #[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
    {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        let elapsed = ORIGIN.get_or_init(Instant::now).elapsed();
        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
    }
}

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
fn performance_ms() -> f64 {
    web_sys::window()
        .and_then(|window| window.performance())
        .map(|performance| performance.now())
        .unwrap_or_else(js_sys::Date::now)
}

/// Suspend for `duration`. Browser hosts cannot use `tokio::time::sleep` (no
/// tokio reactor on the dioxus-web executor), so they wait on `setTimeout`.
async fn sleep(duration: Duration) {
    #[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
    {
        let promise = js_sys::Promise::new(&mut |resolve, _reject| {
            let millis = duration.as_millis().min(i32::MAX as u128) as i32;
            match web_sys::window() {
                Some(window) => {
                    drop(
                        window.set_timeout_with_callback_and_timeout_and_arguments_0(
                            &resolve, millis,
                        ),
                    );
                }
                None => {
                    drop(resolve.call0(&resolve));
                }
            }
        });
        // A setTimeout that never fires would hang wait-for forever.
        drop(wasm_bindgen_futures::JsFuture::from(promise).await);
    }
    #[cfg(not(all(target_arch = "wasm32", not(target_os = "wasi"))))]
    {
        tokio::time::sleep(duration).await;
    }
}

/// `wasi:clocks/system-clock.instant`.
#[derive(ComponentType, Lift, Lower, Debug, Clone, Copy)]
#[component(record)]
struct SystemInstant {
    #[component(name = "seconds")]
    seconds: i64,
    #[component(name = "nanoseconds")]
    nanoseconds: u32,
}
