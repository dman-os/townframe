//! intxt's commands, the subcommands of `day x` (`day x intxt install`), hard-coded until command
//! bindings are generated from installed cartridges. `day x <nsid>` reaches a cartridge's
//! operations directly (`day x org.example.intxt.createIntxt '{"title": ".."}'`), so a command is
//! only what the operations cannot do yet: `install` stores the handler component's bytes, which
//! no operation takes from a file.

use crate::client::NodeClient;
use crate::interlude::*;

use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_blobs_core::PutBlob;
use big_cabinet_core::authority::Rights;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::Readiness;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::SetNoduleOperationGrants;
use method_core::Method;

#[derive(Debug, clap::Subcommand)]
pub enum IntxtCommands {
    /// Admit intxt's cartridge to this node, host its handler, and enable it
    ///
    /// Stores the built handler component in the node's blob store, admits the cartridge that
    /// names it, grants the nodule the operations it calls (the node's `nodule_operations`
    /// ceiling in node.json must cover them), and enables it, which registers intxt's records and
    /// serves `createIntxt`, `addToIntxt` and `recordObservations`.
    Install {
        /// The handler component: `src/wasm_utils_rs/target/wasm32-wasip3/release/intxt_guest.wasm`,
        /// built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_guest
        /// --target wasm32-wasip3 --release`
        #[arg(long)]
        handler: PathBuf,
    },
}

pub async fn run(command: IntxtCommands) -> Res<ExitCode> {
    let client = lazy::node().await?;
    match command {
        IntxtCommands::Install { handler } => {
            let wasm = std::fs::read(&handler)
                .wrap_err_with(|| format!("reading the handler {}", handler.display()))?;
            let record = install(&client, &wasm).await?;
            eyre::ensure!(
                record.readiness == Readiness::Ready,
                "intxt is installed but not ready: {:?}",
                record.readiness
            );
            println!("intxt is installed");
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Installs intxt with the handler component `wasm`, the way a node hosts any nodule's handler
/// (`big_node`'s `nodule_wasm` e2e): the bytes go into the blob store first, so the executor finds
/// them when the nodule is enabled; the cartridge's artifact names them by their BLAKE3.
///
/// Returns the nodule's record. Its readiness says whether the executor loaded the handler: a
/// component the host cannot run is `ExecutorUnavailable`, which is the record's to report, not
/// an error here.
pub(crate) async fn install(client: &NodeClient, wasm: &[u8]) -> Res<NoduleRecord> {
    eyre::ensure!(
        wasm.len() <= big_blobs_core::MAX_PUT_BYTES,
        "the handler is {} bytes, over the {} one blob put carries",
        wasm.len(),
        big_blobs_core::MAX_PUT_BYTES
    );
    let capability = call(
        client,
        MintBlobsCapability {
            scope: BlobsScope {
                put: true,
                ..BlobsScope::default()
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id;
    call(
        client,
        PutBlob {
            capability,
            bytes: wasm.to_vec().into(),
        },
    )
    .await?;

    let cartridge = intxt_nodule::cartridge(intxt_nodule::handler_artifact(wasm));
    let cartridge_id = cartridge.id.clone();
    call(
        client,
        AdmitNodule {
            cartridge,
            drawers: None,
        },
    )
    .await?;
    call(
        client,
        SetNoduleOperationGrants {
            cartridge_id: cartridge_id.clone(),
            operations: DispatchScope {
                operations: intxt_nodule::REQUIRED_OPERATIONS.map(str::to_owned).into(),
                ..DispatchScope::default()
            },
        },
    )
    .await?;
    let enabled = call(
        client,
        SetNoduleEnabled {
            cartridge_id,
            enabled: true,
        },
    )
    .await?;
    // A nodule holds its dispatch capability only while enabled, so whether the grant took is
    // known from the enabled record: the node's ceiling (`nodule_operations` in node.json) must
    // cover what intxt's handler calls.
    let grants = enabled
        .operations
        .as_ref()
        .ok_or_else(|| eyre::eyre!("the node reports no operation grants for intxt"))?;
    eyre::ensure!(
        grants.granted,
        "the node did not grant intxt its operations ({}); the `nodule_operations` of node.json is the ceiling of what a nodule can be granted and must cover {:?}",
        grants.error.as_deref().unwrap_or("no reason given"),
        intxt_nodule::REQUIRED_OPERATIONS
    );
    Ok(enabled)
}

/// Calls `input`'s operation and decodes its answer; a non-success answer is the operation's own
/// error, reported as the node gave it.
pub(crate) async fn call<M>(client: &NodeClient, input: M) -> Res<M::Output>
where
    M: Method,
    M::Output: serde::de::DeserializeOwned,
{
    let reply = client
        .call(M::XRPC.nsid, M::XRPC.method, &serde_json::to_value(input)?)
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "{} failed with {}: {}",
        M::XRPC.nsid,
        reply.status,
        reply.body
    );
    Ok(serde_json::from_value(reply.body)?)
}
