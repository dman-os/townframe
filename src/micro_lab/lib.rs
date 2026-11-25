//! The lab, authored once against the MDK's [`Context`]: the claytxt lane's experiments in one
//! panel. Each experiment is a plain Dioxus component the lab lists; opening one replaces the
//! list, and the experiment's own navigation returns to it. Nothing here is a product surface: an
//! experiment is thin on purpose and is deleted or replaced when the lane that owns the real
//! thing (the mock review's is the intext lane) lands it.
//!
//! [`clay_mdk::export_micro!`] exports the lab as the `micro-client` guest on wasm32-wasip3 and
//! as the native `ACTIVATE` entry everywhere else.
use clay_dioxus_kit::AppBar;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::hooks::Stylesheet;
use dioxus::prelude::*;
use micro_lab_manifest::LabView;
use pierre_diffs_dioxus::PtrEq;
use pierre_diffs_dioxus::Virtualization;
use std::rc::Rc;

/// One experiment of the lab: what the list says about it and the component it opens.
struct Experiment {
    id: &'static str,
    title: &'static str,
    summary: &'static str,
    /// The experiment's content, given the callback that closes it. The experiment owns its app
    /// bar, so it can put the close button where its own navigation puts "back" at the root.
    open: fn(Callback<()>) -> Element,
}

/// The lab's experiments, in list order. Add one by appending an entry.
const EXPERIMENTS: &[Experiment] = &[
    Experiment {
        id: "mock-review",
        title: "Mock review",
        summary: "The review loop's journeys over mock data: changes, an overview, a file diff with annotation threads, a thread.",
        open: |close| rsx! { review_mock::MockReview { close } },
    },
    Experiment {
        id: "large-diff",
        title: "Large diff",
        summary: "About three thousand lines in a virtualized unified diff, the theme lab's large specimen run in the micro.",
        open: |close| rsx! { LargeDiff { close } },
    },
];

/// The large diff with the micro's virtualization, paced by the micro's own timer.
#[component]
fn LargeDiff(close: Callback<()>) -> Element {
    let context = use_context::<Context>();
    // Built once: the view compares the sleep function by identity, and a new one would remount
    // the diff on every render.
    let virtualized = use_hook(|| Virtualization {
        height: "32rem".to_owned(),
        sleep: PtrEq::new(Rc::new(move |duration| {
            let context = context.clone();
            Box::pin(async move { context.sleep(duration).await })
        })),
    });
    rsx! { review_mock::LargeDiff { close, virtualized } }
}

/// Publish the lab's one presentation. The lab needs no capability from the composition.
async fn activate(context: Context) -> Result<(), Error> {
    let provided = context.clone();
    context.publish::<LabView>(move || view(&provided))?;
    Ok(())
}

clay_mdk::export_micro!(activate, micro_lab_manifest::manifest);

/// The lab's own stylesheet: the layout of its parts and the look of an experiment's row, which the
/// kit has no component for. It names no colour that is not a kit token.
const STYLES: &str = include_str!("styles.css");

/// The lab's model and view in one root-scope render: the list of experiments, or the open one.
fn view(context: &Context) -> Element {
    // The experiments reach the micro's clock through the context, not through their props.
    use_context_provider(|| context.clone());
    let mut open = use_signal(|| None::<&'static str>);
    let experiment =
        open().and_then(|id| EXPERIMENTS.iter().find(|experiment| experiment.id == id));
    match experiment {
        Some(experiment) => (experiment.open)(Callback::new(move |()| open.set(None))),
        None => rsx! {
            div { class: "ct-lab",
                Stylesheet { css: STYLES }
                AppBar { title: "Lab" }
                ul { "part": "lab-experiments", "aria-label": "Experiments",
                    for experiment in EXPERIMENTS {
                        li { key: "{experiment.id}",
                            button {
                                "part": "lab-experiment",
                                r#type: "button",
                                "aria-label": "Open {experiment.title}",
                                onclick: move |_| open.set(Some(experiment.id)),
                                strong { "{experiment.title}" }
                                p { "{experiment.summary}" }
                            }
                        }
                    }
                }
            }
        },
    }
}
