pub use clay_rt_wcmp::ActivationError;
pub use clay_rt_wcmp::WcmpRuntime;
use wcmp::Engine;

pub struct WcmpPackage(clay_rt_wcmp::WcmpPackage);

impl WcmpPackage {
    pub async fn load(bytes: &[u8]) -> Result<Self, String> {
        let backend = wcmp_wasm_core_wasmi::Wasmi::new();
        let engine = Engine::with_backend(backend).map_err(|error| error.to_string())?;
        clay_rt_wcmp::WcmpPackage::load(engine, bytes)
            .await
            .map(Self)
    }

    /// Load a component artifact prepared by `x/check-dayframe.ts`, named by the environment
    /// variable `DAYFRAME_<name>_COMPONENT` (uppercased, `-` → `_`; e.g. `slots` →
    /// `DAYFRAME_SLOTS_COMPONENT`). Test and fixture code only: compositions that ship a package
    /// embed or fetch it themselves.
    pub async fn from_env(name: &str) -> Result<Self, String> {
        let variable = format!(
            "DAYFRAME_{}_COMPONENT",
            name.to_uppercase().replace('-', "_")
        );
        let path = std::env::var(&variable).map_err(|_| {
            format!(
                "{variable} must name a prepared {name} component; run deno run --allow-all x/check-dayframe.ts --prepare-only and export the printed path"
            )
        })?;
        let path = std::path::PathBuf::from(path);
        if !path.is_absolute() {
            return Err(format!(
                "{variable} must be an absolute path: {}",
                path.display()
            ));
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("cannot read {variable} at {}: {error}", path.display()))?;
        Self::load(&bytes).await
    }

    pub fn as_shared(&self) -> &clay_rt_wcmp::WcmpPackage {
        &self.0
    }
}

#[cfg(test)]
mod documents_tests;
#[cfg(test)]
mod tests;
