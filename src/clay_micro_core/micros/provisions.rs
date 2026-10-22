//! Live native provisions retained by the host registry, not a service enumeration API.
#[cfg(test)]
mod tests;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

/// An explicit contract implemented by a published native facility. The identity also names the
/// dependency in configuration; native Rust types are adapters, not transport values.
pub trait Provision: Clone + Send + Sync + 'static {
    const ID: &'static str;
    const VERSION: u32 = 1;
}

#[derive(Clone)]
pub(super) struct Published {
    pub host: Host,
    pub owner: Instance,
    pub version: u32,
    value: Arc<dyn Any + Send + Sync>,
}

impl Published {
    /// The stored value behind a contract named at runtime. The caller knows the contract only
    /// by id and version — an MDK envelope resolving a manifest's contribution target — and
    /// downcasts to whatever it stored.
    pub fn get_erased(&self, version: u32) -> Result<Arc<dyn Any + Send + Sync>, AccessError> {
        self.host.check(&self.owner)?;
        assert_eq!(self.version, version, "native contract version mismatch");
        Ok(self.value.clone())
    }

    pub fn get<T: Provision>(&self) -> Result<T, AccessError> {
        Ok(self
            .get_erased(T::VERSION)?
            .downcast_ref::<T>()
            .expect("contract identity mapped to two native types")
            .clone())
    }
}

#[derive(Clone, Default)]
pub(super) struct Provisions(Arc<Mutex<BTreeMap<(String, String), Published>>>);

impl Provisions {
    pub fn resolve(&self, instance: &str, contract: &str) -> Result<Published, AccessError> {
        let records = self.0.lock().unwrap();
        let record = records
            .get(&(instance.into(), contract.into()))
            .ok_or(AccessError::Revoked)?;
        record.host.check(&record.owner)?;
        Ok(record.clone())
    }

    pub fn get_erased(
        &self,
        instance: &str,
        contract: &str,
        version: u32,
    ) -> Result<Arc<dyn Any + Send + Sync>, AccessError> {
        self.resolve(instance, contract)?.get_erased(version)
    }

    pub fn get<T: Provision>(&self, instance: &str) -> Result<T, AccessError> {
        self.resolve(instance, T::ID)?.get::<T>()
    }

    pub fn commit(
        &self,
        host: &Host,
        owner: &Instance,
        values: BTreeMap<String, Arc<dyn Any + Send + Sync>>,
        descriptor: &super::MicroDescriptor,
    ) -> Result<(), AccessError> {
        let keys: Vec<_> = values
            .keys()
            .map(|id| (owner.name.clone(), id.clone()))
            .collect();
        // Hold publication storage across cleanup registration and insertion. If the owner closes
        // concurrently, cleanup waits for the committed records rather than running before insertion.
        let mut published = self.0.lock().unwrap();
        let records = self.0.clone();
        let incarnation = owner.clone();
        // Cleanup compares runtime identities so stale cleanup cannot withdraw a replacement.
        host.own(
            owner,
            Box::new(move || {
                let mut records = records.lock().unwrap();
                for key in keys {
                    if records
                        .get(&key)
                        .is_some_and(|record| record.owner == incarnation)
                    {
                        records.remove(&key);
                    }
                }
            }),
        )?;
        for (id, value) in values {
            let version = descriptor
                .provisions
                .iter()
                .find(|provision| provision.id == id)
                .unwrap()
                .version;
            assert!(
                published
                    .insert(
                        (owner.name.clone(), id),
                        Published {
                            host: host.clone(),
                            owner: owner.clone(),
                            version,
                            value,
                        }
                    )
                    .is_none(),
                "configured instance already has a live provision"
            );
        }
        Ok(())
    }
}
