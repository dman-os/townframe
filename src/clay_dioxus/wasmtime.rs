use crate::NativeComponents;
use clay_compose::micros::{
    ActivationError, MicroActivation, MicroFactory, MicroManifest, Provision,
};
use clay_data::DocumentRef;
pub use clay_engine_wasmtime::WasmPackage;
use clay_engine_wasmtime::WasmRuntime;
use clay_host::{AccessError, ComponentRef, Instance};
use std::{collections::BTreeMap, rc::Rc, sync::Arc};

#[derive(Clone)]
pub struct WasmMicro {
    pub instance: Instance,
    runtime: WasmRuntime,
    detail: ComponentRef,
}

impl PartialEq for WasmMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl Provision for WasmMicro {
    const ID: &'static str = "wasm.controller";
}

impl WasmMicro {
    pub fn activate(
        activation: &MicroActivation,
        package: &WasmPackage,
        native: &NativeComponents,
        configs: BTreeMap<String, DocumentRef>,
    ) -> Result<Self, ActivationError> {
        let instance = activation.instance();
        // The engine checks component grants against the host this renderer registry serves.
        let (runtime, driver) =
            WasmRuntime::activate(&native.host, instance.clone(), package, configs).map_err(
                |error| match error {
                    clay_engine_wasmtime::ActivationError::Access(error) => {
                        ActivationError::Access(error)
                    }
                    clay_engine_wasmtime::ActivationError::Initialization(message) => {
                        ActivationError::Initialization(message)
                    }
                },
            )?;
        activation.own_future(driver)?;
        let mounted = runtime.clone();
        let detail = native.publish_surface(&instance, move |_mount, writer| {
            mounted.mount(Arc::new(writer), &[])
        })?;
        Ok(Self {
            instance,
            runtime,
            detail,
        })
    }

    pub fn surface(&self) -> ComponentRef {
        self.detail
    }

    pub fn exhaust_fuel(&self) {
        self.runtime.exhaust_fuel();
    }
}

pub fn provider<C: 'static>(
    manifest: MicroManifest<C>,
    package: WasmPackage,
    native: NativeComponents,
    contribute: impl Fn(&MicroActivation, &C, ComponentRef) -> Result<(), AccessError> + 'static,
) -> MicroFactory {
    let descriptor = manifest.descriptor.clone();
    assert!(
        descriptor.provisions.iter().any(|provision| {
            provision.id == WasmMicro::ID && provision.version == WasmMicro::VERSION
        }),
        "Wasm manifest must declare its controller provision"
    );
    let manifest = Rc::new(manifest);
    let contribute = Rc::new(contribute);
    MicroFactory::new_async(descriptor, move |activation| {
        let manifest = manifest.clone();
        let package = package.clone();
        let native = native.clone();
        let contribute = contribute.clone();
        async move {
            let configs = match &manifest.config {
                Some(config) => activation.grant_config(&config.name, config.facets.clone())?,
                None => BTreeMap::new(),
            };
            let micro = WasmMicro::activate(&activation, &package, &native, configs)?;
            for contribution in &manifest.contributions {
                contribute(&activation, contribution, micro.surface())?;
            }
            activation.publish(micro);
            Ok(())
        }
    })
}

#[cfg(test)]
#[path = "wasmtime_tests.rs"]
mod tests;
