use std::{path::PathBuf, sync::LazyLock};

pub fn counter_component() -> &'static [u8] {
    static BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let path = PathBuf::from(std::env::var_os("DAYFRAME_COUNTER_COMPONENT").expect(
            "DAYFRAME_COUNTER_COMPONENT must name a prepared counter component; run deno run --allow-all x/check-dayframe.ts --prepare-only and export the printed path",
        ));
        assert!(
            path.is_absolute(),
            "DAYFRAME_COUNTER_COMPONENT must be an absolute path: {}",
            path.display()
        );
        std::fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "cannot read DAYFRAME_COUNTER_COMPONENT at {}: {error}; run deno run --allow-all x/check-dayframe.ts --prepare-only",
                path.display()
            )
        })
    });
    BYTES.as_slice()
}
