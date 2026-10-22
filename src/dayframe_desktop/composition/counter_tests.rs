use super::*;
use clay_dioxus::NativeComponents;
use clay_host::AccessError;
use clay_host::ComponentRef;
use clay_host::Instance;
use clay_mdk::native::output;
use clay_mdk::wasm::WasmMicro;
use clay_surface::SurfaceTree;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_html::EventData;
use dioxus_html::SerializedMouseData;
use micro_settings::SettingsMicro;

#[derive(Clone, PartialEq)]
struct MountProps {
    components: NativeComponents,
    owner: Instance,
    reference: ComponentRef,
}

fn mount_host_app(props: MountProps) -> Element {
    rsx! { div {
        ComponentMount { components: props.components, owner: props.owner, reference: props.reference }
    } }
}

fn click(dom: &mut VirtualDom, tree: &mut SurfaceTree) {
    let event = dioxus_core::Event::new(
        EventData::Mouse(SerializedMouseData::default()).into_any(),
        true,
    );
    dom.runtime().handle_event_for_target(
        RenderTargetId::ROOT,
        "click",
        event,
        tree.listener_id("button", "click").unwrap(),
    );
    dom.render_immediate(tree);
}

#[test]
fn counter_manifest_contributes_live_settings_and_withdraws_on_trap_or_disablement() {
    let mut runtime = DesktopRuntime::new();
    runtime.apply_now().unwrap();
    let app = runtime.application.clone();
    let settings = output::<SettingsMicro, _>(&app, "settings").unwrap();
    let first = app.output::<WasmMicro>("counter").unwrap();
    let sections = settings.sections.snapshot().unwrap();
    assert_eq!(sections.len(), 2);
    let section = sections
        .values()
        .find(|section| section.title == "Counter (Wasm)")
        .unwrap();
    assert_eq!(section.detail, first.surface());
    let mut dom = VirtualDom::new_with_props(
        mount_host_app,
        MountProps {
            components: app.renderer(),
            owner: settings.instance.clone(),
            reference: section.detail.host(),
        },
    );
    let mut tree = SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert_eq!(
        tree.text_content(),
        "0Pending counter writes: 0Refresh counter"
    );
    click(&mut dom, &mut tree);
    let text = tree.text_content();
    assert!(
        text.starts_with("1Pending counter writes: 1Operation ")
            && text.ends_with(": OperationStatus::LocallyAppliedRefresh counter"),
        "optimistic write renders its LocallyApplied receipt: {text}"
    );
    first.exhaust_fuel();
    click(&mut dom, &mut tree);
    assert_eq!(app.host().check(&first.instance), Err(AccessError::Revoked));
    assert_eq!(
        settings
            .sections
            .snapshot()
            .unwrap()
            .into_values()
            .map(|entry| entry.title)
            .collect::<Vec<_>>(),
        vec!["Sample micro"]
    );
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["counter"], InstanceState::Stopped);
    runtime
        .set_enablement("counter", Enablement::Disabled)
        .unwrap();
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["counter"], InstanceState::Disabled);
    runtime
        .set_enablement("counter", Enablement::Enabled)
        .unwrap();
    runtime.apply_now().unwrap();
    assert_eq!(runtime.states["counter"], InstanceState::Active);
    let second = app.output::<WasmMicro>("counter").unwrap();
    assert!(first.instance != second.instance);
    let sections = settings.sections.snapshot().unwrap();
    assert_eq!(sections.len(), 2);
    assert!(
        sections
            .values()
            .any(|entry| entry.detail == second.surface())
    );
    app.host().close();
}
