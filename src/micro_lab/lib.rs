//! The lab, authored once against the MDK's [`Context`]: the claytxt lane's experiments in one
//! panel. Each experiment is a plain Dioxus component the lab lists; opening one replaces the
//! list, and the experiment's own navigation returns to it. Nothing here is a product surface: an
//! experiment is thin on purpose and is deleted or replaced when the lane that owns the real
//! thing (the mock review's is the intext lane) lands it.
//!
//! [`clay_mdk::export_micro!`] exports the lab as the `micro-client` guest on wasm32-wasip3 and
//! as the native `ACTIVATE` entry everywhere else.
mod mock_review;

use clay_dioxus_kit::AppBar;
use clay_mdk::Context;
use clay_mdk::Error;
use dioxus::prelude::*;
use micro_lab_manifest::LabView;

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
const EXPERIMENTS: &[Experiment] = &[Experiment {
    id: "mock-review",
    title: "Mock review",
    summary: "The review loop's journeys over mock data: changes, an overview, a file diff with annotation threads, a thread.",
    open: |close| rsx! { mock_review::MockReview { close } },
}];

/// Publish the lab's one presentation. The lab needs no capability from the composition.
async fn activate(context: Context) -> Result<(), Error> {
    context.publish::<LabView>(view)?;
    Ok(())
}

clay_mdk::export_micro!(activate, micro_lab_manifest::manifest);

/// The lab's model and view in one root-scope render: the list of experiments, or the open one.
fn view() -> Element {
    let mut open = use_signal(|| None::<&'static str>);
    let experiment = open().and_then(|id| EXPERIMENTS.iter().find(|experiment| experiment.id == id));
    match experiment {
        Some(experiment) => (experiment.open)(Callback::new(move |()| open.set(None))),
        None => rsx! {
            div { class: "flex flex-col gap-4",
                AppBar { title: "Lab" }
                ul { class: "mx-4 flex flex-col gap-2", "aria-label": "Experiments",
                    for experiment in EXPERIMENTS {
                        li { key: "{experiment.id}",
                            button {
                                class: "block min-h-12 w-full rounded-md border border-border px-4 py-3 text-left focus-visible:outline",
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
