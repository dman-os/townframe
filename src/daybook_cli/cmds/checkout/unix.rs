use super::*;
use daybook_types::doc::{
    BranchPath, ChangeHashSet, DocPatch, FacetKey, WellKnownFacet, WellKnownFacetTag,
};
use daybook_types::dpath::{Dpath, DpathFacet};
use daybook_types::url::build_facet_ref;
use pauperfuse::backends::tokio_fs::{
    CollectedFile, ExpectedFile, FileEvidence, FilePut, FileTake, TokioFs,
};
use pauperfuse::backends::{
    BackendId, BackendTree, Description, Producer, ProducerAccess, RelPath,
};
use pauperfuse::vtree::VtreeStore;
use pauperfuse_daybook::{
    Daybook, MAX_RAW_NOTE_BYTES, Projection, prepare_note_ingest, validate_note,
};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use tokio::io::AsyncWriteExt;
mod ingest;
mod lifecycle;
mod observe;
mod publish;
mod state;

use ingest::*;
use lifecycle::*;
use observe::*;
use publish::*;
use state::*;

pub(super) async fn run(command: DtreeCommands) -> Res<ExitCode> {
    match command {
        DtreeCommands::Create { directory, doc } => {
            ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
            let context = lazy::repo_ctx().await?;
            let drawer = lazy::drawer_repo().await?;
            let root = create(node_handle(&context), drawer, &directory, doc).await?;
            println!("created checkout {}", root.display());
        }
        DtreeCommands::Ingest {
            directory,
            allow,
            adopt_import_claim,
            drop_import_claim,
        } => {
            ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
            let start = directory.unwrap_or(std::env::current_dir()?);
            let (root, mut marker) = discover(&start).await?;
            lazy::select_checkout_repo(marker.checkout().node_path()?);
            let context = lazy::repo_ctx().await?;
            verify_node(&node_handle(&context), marker.checkout()).await?;
            let drawer = lazy::drawer_repo().await?;
            resolve_claims(
                &root,
                &mut marker,
                drawer.as_ref(),
                &adopt_import_claim,
                &drop_import_claim,
            )
            .await?;
            ingest_checked(&root, &mut marker, drawer.as_ref(), &allow).await?;
        }
        DtreeCommands::Publish { directory } => {
            ensure_checkout_support(lazy::plugs_repo().await?.as_ref()).await?;
            let start = directory.unwrap_or(std::env::current_dir()?);
            let (root, mut marker) = discover(&start).await?;
            lazy::select_checkout_repo(marker.checkout().node_path()?);
            let context = lazy::repo_ctx().await?;
            verify_node(&node_handle(&context), marker.checkout()).await?;
            let drawer = lazy::drawer_repo().await?;
            verify_bindings(drawer.as_ref(), marker.checkout()).await?;
            // Publish is the upgrade point: a pre-publication (v2) marker
            // becomes a v3 marker on this verb's first write.
            marker.upgrade();
            publish_pipeline(&root, &mut marker, &drawer).await?;
        }
        DtreeCommands::Status { directory } => {
            let start = directory.unwrap_or(std::env::current_dir()?);
            let (root, marker) = discover(&start).await?;
            lazy::select_checkout_repo(marker.checkout().node_path()?);
            let context = lazy::repo_ctx().await?;
            verify_node(&node_handle(&context), marker.checkout()).await?;
            let drawer = lazy::drawer_repo().await?;
            verify_bindings(drawer.as_ref(), marker.checkout()).await?;
            for line in status(&root, marker.checkout(), drawer.as_ref()).await? {
                println!("{line}");
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests;
