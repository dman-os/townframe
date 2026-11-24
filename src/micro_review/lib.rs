//! The intxt review micro (ledger D62): read a GitHub pull request on the phone and find the
//! intxts it names. Three screens in one presentation, [`micro_review_manifest::VIEW`]: the pull
//! requests the node holds, one pull request with its description and the intxts its description
//! block names, and one intxt with its guide and its entries, a commit entry's patch in the diff
//! view.
//!
//! Everything the screens show is read through [`node::NodeCalls`] in a resource, so a screen
//! renders at once and fills in when the node answers; nothing awaits in mount or dispatch.
//! [`model`] holds what each screen reads and the rules of its rows; [`view`] draws them.
//! [`clay_mdk::export_micro!`] exports the micro as the `micro-client` guest on wasm32-wasip3 and
//! as the native `ACTIVATE` entry everywhere else.
//!
//! The manifest declares a `layout.panels` row, so the layout lists the micro as a panel. It
//! reads its PRs from the GitHub drawer its install configuration names ([`node::Configuration`]).
#[cfg(any(test, feature = "fixture"))]
pub mod fixture;
pub mod model;
pub mod node;
mod view;

use clay_dioxus_kit::AppBar;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use clay_mdk::hooks::use_facet;
use dioxus::prelude::*;
use micro_review_manifest::ReviewConfig;
use micro_review_manifest::ReviewConfigDecl;
use micro_review_manifest::ReviewView;
use node::CallError;
use node::ContextNode;
use view::Failed;
use view::Frame;

/// Publish the presentation over the node the micro is configured to read: the real node
/// through the micro's `Context` when the composition granted the configuration entry. With none
/// granted it publishes the "Unconfigured" alert: sample pull requests on a phone would read as
/// real, so the fixture node ([`fixture`], behind the `fixture` feature and tests) is never a
/// fallback here.
async fn activate(context: Context) -> Result<(), Error> {
    match context.config::<ReviewConfigDecl>()? {
        Some(facet) => context.publish::<ReviewView>({
            let context = context.clone();
            move || configured_view(context.clone(), facet.clone())
        })?,
        None => context.publish::<ReviewView>(|| {
            rsx! {
                Frame { app_bar: rsx! { AppBar { title: "Review" } },
                    Failed {
                        error: CallError::Unconfigured(
                            "grant the micro the org.example.dayframe.review.config entry (githubDrawer, intxtDrawers, docCap, blobCap)"
                                .to_owned(),
                        ),
                    }
                }
            }
        })?,
    }
    Ok(())
}

clay_mdk::export_micro!(activate, micro_review_manifest::manifest);

/// [`view::screens`] over the real node. Binding the facet to reactivity makes a configuration
/// entry written, or changed, after mount re-render the view over a node that reads the new one.
fn configured_view(context: Context, facet: Facet<ReviewConfig>) -> Element {
    let snapshot = use_facet(&facet);
    view::screens(ContextNode::new(context, snapshot.state))
}
