pub use clay_rt_wcmp::ActivationError;
pub use clay_rt_wcmp::WcmpRuntime;
use wcmp::Engine;

pub struct WcmpPackage(clay_rt_wcmp::WcmpPackage);

impl WcmpPackage {
    pub async fn load(bytes: &[u8]) -> Result<Self, String> {
        let backend = wcmp_wasm_core_web::Web::new();
        let engine = Engine::with_backend(backend).map_err(|error| error.to_string())?;
        clay_rt_wcmp::WcmpPackage::load(engine, bytes)
            .await
            .map(Self)
    }

    pub fn as_shared(&self) -> &clay_rt_wcmp::WcmpPackage {
        &self.0
    }
}
