//! Engine-neutral presentation host state (ADR 018 §2): surface and placement
//! resource state, component grant checks, handle caps, and the ingress budget.
//!
//! Engine adapters (`clay_rt_wasmtime`, `clay_rt_wcmp`) own only their generated
//! bindings, resource tables, and store construction. They check the wire ingress
//! budget before resolving resources, convert mutations, then delegate the remaining
//! semantic decisions here. Engine types never enter this module (ADR 015 boundary
//! correction).

use clay_host::AccessError;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::Instance;
use clay_surface::AttributeValue;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::SurfaceSink;
use clay_surface::SurfaceTree;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Host side of one `mount-point` resource: the writer a guest may open exactly
/// one surface into, plus the revocation flag shared with that surface.
pub struct Placement {
    writer: Arc<dyn SurfaceSink<ComponentRef>>,
    revoked: Arc<AtomicBool>,
    surface_identity: Option<u64>,
}

impl Placement {
    pub fn new(writer: Arc<dyn SurfaceSink<ComponentRef>>, revoked: Arc<AtomicBool>) -> Self {
        Self {
            writer,
            revoked,
            surface_identity: None,
        }
    }

    /// Identity of the surface opened from this placement, once opened. The
    /// engine adapter compares it against the surface a guest's `mount` call
    /// returns, so a guest cannot substitute a surface from another placement.
    pub fn surface_identity(&self) -> Option<u64> {
        self.surface_identity
    }
}

/// Host side of one `surface` resource. Cloning shares ownership: the writer is
/// withdrawn when the last clone drops, which is how a guest dropping its final
/// surface handle tears down its rendering.
#[derive(Clone)]
pub struct Surface {
    writer: Arc<dyn SurfaceSink<ComponentRef>>,
    revoked: Arc<AtomicBool>,
    identity: u64,
    ownership: Arc<()>,
}

impl Surface {
    pub fn identity(&self) -> u64 {
        self.identity
    }

    pub fn writer(&self) -> &Arc<dyn SurfaceSink<ComponentRef>> {
        &self.writer
    }
}

impl Drop for Surface {
    fn drop(&mut self) {
        if Arc::strong_count(&self.ownership) == 1 {
            self.writer.withdraw();
        }
    }
}

/// Per-incarnation presentation state shared by every engine adapter.
pub struct PresentationHost {
    host: Host,
    instance: Instance,
    next_surface: u64,
    surface_handles: usize,
    component_handles: usize,
}

impl PresentationHost {
    pub const MAX_SURFACE_HANDLES: usize = 1024;
    pub const MAX_COMPONENT_HANDLES: usize = 1024;

    pub fn new(host: Host, instance: Instance) -> Self {
        Self {
            host,
            instance,
            next_surface: 0,
            surface_handles: 0,
            component_handles: 0,
        }
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn instance(&self) -> &Instance {
        &self.instance
    }

    pub fn check(&self) -> Result<(), AccessError> {
        self.host.check(&self.instance)
    }

    pub fn surface_handles(&self) -> usize {
        self.surface_handles
    }

    pub fn component_handles(&self) -> usize {
        self.component_handles
    }

    /// Reserve capacity for `count` component handles a guest call is about to
    /// receive. The adapter pushes that many resources into its table (which
    /// cannot fail once capacity is reserved) and calls
    /// [`Self::release_component`] as each handle drops.
    pub fn admit_components(&mut self, count: usize) -> Result<(), PresentationError> {
        if count > Self::MAX_COMPONENT_HANDLES - self.component_handles {
            return Err(PresentationError::Unsupported(
                "component handle limit".into(),
            ));
        }
        self.component_handles += count;
        Ok(())
    }

    pub fn release_component(&mut self) {
        self.component_handles -= 1;
    }

    /// Open the single surface a placement grants, checking revocation and the
    /// surface handle cap.
    pub fn open(&mut self, placement: &mut Placement) -> Result<Surface, PresentationError> {
        self.check().map_err(|_| PresentationError::Revoked)?;
        if placement.revoked.load(Ordering::Acquire) || !placement.writer.is_live() {
            return Err(PresentationError::Revoked);
        }
        if placement.surface_identity.is_some() {
            return Err(PresentationError::Unsupported(
                "placement already opened".into(),
            ));
        }
        if self.surface_handles >= Self::MAX_SURFACE_HANDLES {
            return Err(PresentationError::Unsupported(
                "surface handle limit".into(),
            ));
        }
        self.next_surface = self
            .next_surface
            .checked_add(1)
            .expect("surface identity overflow");
        placement.surface_identity = Some(self.next_surface);
        self.surface_handles += 1;
        Ok(Surface {
            writer: Arc::clone(&placement.writer),
            revoked: Arc::clone(&placement.revoked),
            identity: self.next_surface,
            ownership: Arc::new(()),
        })
    }

    /// Apply one converted mutation batch, enforcing the ingress budget and the
    /// component grants of every `MountComponent` it carries. A budget violation
    /// withdraws the surface: the guest has broken the batch contract.
    pub fn apply(
        &self,
        surface: &Surface,
        edits: &[Mutation<ComponentRef>],
    ) -> Result<u64, PresentationError> {
        if let Err(reason) = ingress_budget(edits) {
            surface.writer.withdraw();
            return Err(PresentationError::Unsupported(reason.into()));
        }
        self.check().map_err(|_| PresentationError::Revoked)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Err(PresentationError::Revoked);
        }
        for mutation in edits {
            if let Mutation::MountComponent(reference) = mutation {
                self.host
                    .check_component(*reference)
                    .map_err(|error| match error {
                        AccessError::Revoked => PresentationError::Revoked,
                        AccessError::Denied => PresentationError::InvalidBatch(
                            "component grant belongs to another host".into(),
                        ),
                    })?;
            }
        }
        surface.writer.apply(edits)
    }

    /// Retain a surface as an additional handle, under the same cap as `open`.
    pub fn retain(&mut self, surface: &Surface) -> Result<Surface, PresentationError> {
        self.check().map_err(|_| PresentationError::Revoked)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Err(PresentationError::Revoked);
        }
        if self.surface_handles >= Self::MAX_SURFACE_HANDLES {
            return Err(PresentationError::Unsupported(
                "surface handle limit".into(),
            ));
        }
        self.surface_handles += 1;
        Ok(surface.clone())
    }

    pub fn identity(&self, surface: &Surface) -> Result<u64, AccessError> {
        self.check()?;
        Ok(surface.identity)
    }

    /// Release one surface handle previously counted by `open` or `retain`.
    pub fn release_surface(&mut self) {
        self.surface_handles -= 1;
    }
}

/// Resolve component references while preserving the rest of each mutation. Adapters
/// call this only after the raw batch passes [`ingress_budget`].
pub fn map_mutations<R, C, E>(
    edits: Vec<Mutation<R>>,
    mut resolve: impl FnMut(R) -> Result<C, E>,
) -> Result<Vec<Mutation<C>>, E> {
    edits
        .into_iter()
        .map(|mutation| {
            Ok(match mutation {
                Mutation::PushId(id) => Mutation::PushId(id),
                Mutation::SetId(id) => Mutation::SetId(id),
                Mutation::Child(index) => Mutation::Child(index),
                Mutation::Pop => Mutation::Pop,
                Mutation::CreateElement(element) => Mutation::CreateElement(element),
                Mutation::CreateText(text) => Mutation::CreateText(text),
                Mutation::MountComponent(reference) => {
                    Mutation::MountComponent(resolve(reference)?)
                }
                Mutation::CloneNode => Mutation::CloneNode,
                Mutation::AppendChildren(count) => Mutation::AppendChildren(count),
                Mutation::ReplaceWith(count) => Mutation::ReplaceWith(count),
                Mutation::InsertAfter(count) => Mutation::InsertAfter(count),
                Mutation::InsertBefore(count) => Mutation::InsertBefore(count),
                Mutation::SetAttribute(attribute) => Mutation::SetAttribute(attribute),
                Mutation::SetText(text) => Mutation::SetText(text),
                Mutation::AddEventListener(name) => Mutation::AddEventListener(name),
                Mutation::RemoveEventListener(name) => Mutation::RemoveEventListener(name),
                Mutation::Remove => Mutation::Remove,
            })
        })
        .collect()
}

/// Bound one mutation batch before it reaches a surface tree: batch length,
/// per-string bytes, and total bytes, mirroring `SurfaceTree`'s own limits so a
/// hostile guest is rejected at the boundary instead of mid-replay.
pub fn ingress_budget<R>(edits: &[Mutation<R>]) -> Result<(), &'static str> {
    if edits.len() > SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS {
        return Err("batch mutation limit");
    }
    let mut bytes = 0_usize;
    for mutation in edits {
        let strings: [Option<&str>; 3] = match mutation {
            Mutation::CreateElement((tag, namespace)) => [Some(tag), namespace.as_deref(), None],
            Mutation::CreateText(text)
            | Mutation::SetText(text)
            | Mutation::AddEventListener(text)
            | Mutation::RemoveEventListener(text) => [Some(text), None, None],
            Mutation::SetAttribute((name, namespace, value)) => [
                Some(name),
                namespace.as_deref(),
                match value {
                    AttributeValue::Text(text) => Some(text),
                    _ => None,
                },
            ],
            Mutation::PushId(_)
            | Mutation::SetId(_)
            | Mutation::Child(_)
            | Mutation::Pop
            | Mutation::MountComponent(_)
            | Mutation::CloneNode
            | Mutation::AppendChildren(_)
            | Mutation::ReplaceWith(_)
            | Mutation::InsertAfter(_)
            | Mutation::InsertBefore(_)
            | Mutation::Remove => [None, None, None],
        };
        bytes += std::mem::size_of::<Mutation<ComponentRef>>();
        for value in strings.into_iter().flatten() {
            if value.len() > SurfaceTree::<ComponentRef>::MAX_STRING_BYTES {
                return Err("string byte limit");
            }
            bytes += value.len();
        }
        if bytes > SurfaceTree::<ComponentRef>::MAX_BATCH_BYTES {
            return Err("batch byte limit");
        }
    }
    Ok(())
}
