//! Live native provisions retained by the host registry, not a service enumeration API.
#[cfg(test)]
mod tests;
use crate::host::{AccessError, Host, Instance};
use std::{
    any::Any,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// An explicit contract implemented by a published native facility. The identity also names the
/// dependency in configuration; native Rust types are adapters, not transport values.
pub trait Provision: Clone + Send + Sync + 'static {
    const ID: &'static str;
    const VERSION: u32 = 1;
}

#[derive(Clone)]
struct Published {
    owner: Instance,
    version: u32,
    value: Arc<dyn Any + Send + Sync>,
}

#[derive(Clone, Default)]
pub(super) struct Provisions(Arc<Mutex<BTreeMap<(String, String), Published>>>);

impl Provisions {
    pub fn provider(&self, instance: &str, contract: &str) -> Result<(Instance, u32), AccessError> {
        let records = self.0.lock().unwrap();
        let record = records
            .get(&(instance.into(), contract.into()))
            .ok_or(AccessError::Revoked)?;
        record.owner.check()?;
        Ok((record.owner.clone(), record.version))
    }

    pub fn get<T: Provision>(&self, instance: &str) -> Result<T, AccessError> {
        let records = self.0.lock().unwrap();
        let record = records
            .get(&(instance.into(), T::ID.into()))
            .ok_or(AccessError::Revoked)?;
        record.owner.check()?;
        assert_eq!(
            record.version,
            T::VERSION,
            "native contract version mismatch"
        );
        Ok(record
            .value
            .downcast_ref::<T>()
            .expect("contract identity mapped to two native types")
            .clone())
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
