//! Native renderer adapter. Closures stay here; the host boundary carries component references.
use crate::host::{AccessError, ComponentRef, Host, Instance};
use dioxus::prelude::*;
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

type Renderer = Rc<dyn Fn() -> Element>;
#[derive(Clone)]
pub struct NativeComponents {
    host: Host,
    renderers: Rc<RefCell<BTreeMap<ComponentRef, Renderer>>>,
}
impl PartialEq for NativeComponents {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.renderers, &other.renderers)
    }
}
impl NativeComponents {
    /// A renderer may capture this factory, never the registry itself: otherwise publishing
    /// a layout that mounts other content would create a registry → renderer → registry cycle.
    pub(crate) fn weak_context(&self) -> impl Fn() -> Self + 'static {
        let registry = Rc::downgrade(&self.renderers);
        let host = self.host.clone();
        move || Self {
            host: host.clone(),
            renderers: registry
                .upgrade()
                .expect("renderer invoked after native adapter destruction"),
        }
    }
    pub fn new(host: &Host) -> Self {
        Self {
            host: host.clone(),
            renderers: Rc::default(),
        }
    }
    pub fn publish(
        &self,
        owner: &Instance,
        render: impl Fn() -> Element + 'static,
    ) -> Result<ComponentRef, AccessError> {
        self.recover_withdrawn();
        let reference = self.host.publish_component(owner)?;
        assert!(
            self.renderers
                .borrow_mut()
                .insert(reference, Rc::new(render))
                .is_none()
        );
        Ok(reference)
    }
    fn recover_withdrawn(&self) {
        let allowed = self.host.component_lifetimes().borrow().clone();
        self.renderers
            .borrow_mut()
            .retain(|reference, _| allowed.contains(reference));
    }
}

/// The native adapter drops local factories when the host withdraws their portable references.
/// This task belongs to the application render scope; no micro-specific cleanup lives in startup.
pub fn use_component_recovery(components: NativeComponents) {
    use_future(move || {
        let mut lifetimes = components.host.component_lifetimes();
        let registry = Rc::downgrade(&components.renderers);
        async move {
            loop {
                let allowed = lifetimes.borrow_and_update().clone();
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                registry
                    .borrow_mut()
                    .retain(|reference, _| allowed.contains(reference));
                if lifetimes.changed().await.is_err() {
                    break;
                }
            }
        }
    });
}

#[component]
pub fn ComponentMount(
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
) -> Element {
    // A changed reference is a new mount incarnation, even when its collection entry stays put.
    rsx! { MountedComponent { key: "{reference:?}", components, owner, reference } }
}

#[component]
fn MountedComponent(
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
) -> Element {
    let mount = use_hook(|| match components.host.mount(&owner, reference) {
        Ok(mount) => Some(Rc::new(mount)),
        Err(AccessError::Revoked) => None,
        Err(AccessError::Denied) => panic!("native component mount crossed its granted host"),
    });
    let mut live = use_signal(|| mount.as_ref().is_some_and(|mount| mount.is_live()));
    let subscription = mount.clone();
    use_future(move || {
        let mount = subscription.clone();
        async move {
            if let Some(mount) = mount {
                let mut changes = mount.subscribe();
                loop {
                    live.set(*changes.borrow_and_update());
                    if changes.changed().await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    if !live() || !mount.as_ref().is_some_and(|mount| mount.is_live()) {
        return rsx! { p { "Component withdrawn." } };
    }
    let render = components.renderers.borrow().get(&reference).cloned();
    match render {
        Some(render) => render(),
        None => panic!("live component has no native renderer"),
    }
}
