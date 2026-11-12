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
use clay_surface::ElementCommand;
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

    /// Bound a raw guest batch before it is converted: the ingress budget of
    /// [`ingress_budget`]. A breach is the guest's own fault and goes through
    /// [`Self::refuse`].
    pub fn admit<R>(
        &self,
        surface: &Surface,
        edits: &[Mutation<R>],
    ) -> Result<(), PresentationError> {
        ingress_budget(edits)
            .map_err(|reason| self.refuse(surface, PresentationError::Unsupported(reason.into())))
    }

    /// A batch the host refuses for breaking the batch contract (over an ingress or tree
    /// budget, malformed, or naming a component grant of another host) revokes that surface and
    /// faults the micro, with the refusal as the recorded cause, so the supervisor reports it
    /// `Faulted` and the other micros keep running. The guest is answered `Revoked`, not the
    /// refusal: a guest SDK treats `Revoked` as "this surface is gone" and unwinds out of its
    /// call, where it aborts on any other error, and an abort is no better for the cause: the
    /// browser's backend reports a guest trap only when its promise settles, after the
    /// synchronous export's first poll, which `first_poll` takes as a broken contract and
    /// panics the shell on. The cause is recorded here, where the budget is known, because the
    /// first fault of an incarnation stands.
    fn refuse(&self, surface: &Surface, error: PresentationError) -> PresentationError {
        surface.writer.withdraw();
        self.host.record_fault(
            &self.instance,
            format!("guest presentation batch refused: {error}"),
        );
        self.host
            .close_instance(&self.instance)
            .expect("a presentation host closes its own instance");
        PresentationError::Revoked
    }

    /// Apply one converted mutation batch, enforcing the ingress budget and the
    /// component grants of every `MountComponent` it carries, and the surface tree's
    /// own limits. A refused batch faults the micro, see [`Self::refuse`].
    pub fn apply(
        &self,
        surface: &Surface,
        edits: &[Mutation<ComponentRef>],
    ) -> Result<u64, PresentationError> {
        self.admit(surface, edits)?;
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
                        AccessError::Denied => self.refuse(
                            surface,
                            PresentationError::InvalidBatch(
                                "component grant belongs to another host".into(),
                            ),
                        ),
                    })?;
            }
        }
        match surface.writer.apply(edits) {
            Err(
                error @ (PresentationError::Unsupported(_) | PresentationError::InvalidBatch(_)),
            ) => Err(self.refuse(surface, error)),
            applied => applied,
        }
    }

    /// Forward an element command to the surface's renderer, under the revocation checks `apply`
    /// makes. Whether `element` announced `mounted` is the renderer's to decide, and a refusal
    /// goes back to the caller without faulting the micro: unlike a malformed batch, a command
    /// for an element that is gone is a race the guest can observe and report.
    pub fn element_command(
        &self,
        surface: &Surface,
        element: clay_surface::NodeId,
        command: ElementCommand,
    ) -> Result<(), PresentationError> {
        self.check().map_err(|_| PresentationError::Revoked)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Err(PresentationError::Revoked);
        }
        surface.writer.element_command(element, command)
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
                Mutation::SetStyleSheet(sheet) => Mutation::SetStyleSheet(sheet),
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
            | Mutation::SetStyleSheet(text)
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
