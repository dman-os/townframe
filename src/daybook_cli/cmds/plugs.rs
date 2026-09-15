use crate::interlude::*;

use daybook_core::plugs::{OciImportOptions, PlugsRepo};
use daybook_types::doc::ChangeHashSet;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, clap::Subcommand)]
pub enum PlugsCommands {
    /// List known plugs: id, version, status, config doc id
    List,
    /// Show a plug's manifest summary, facets, and refs
    Show {
        /// Plug id (e.g. @daybook/core) or a db+facet:// ref
        target: String,
    },
    /// Enable a plug by facet ref or doc id
    Enable {
        /// db+facet:// ref or bare manifest doc id
        target: String,
        /// Pin to explicit heads (pipe-separated) instead of current main
        #[arg(long)]
        heads: Option<String>,
    },
    /// Disable a plug (rejected for @daybook/core)
    Disable { plug_id: String },
    /// Re-pin a plug to the latest main-branch heads
    Update { plug_id: String },
    /// Enabled entries whose doc/heads are not locally readable
    Pending,
    /// Import a plug from a doc id, facet ref, OCI registry ref, or local OCI layout
    Import {
        /// db+facet:// ref, bare doc id, oci://<registry-ref>, or a local OCI layout path
        target: String,
        /// Leave the plug known but not enabled
        #[arg(long)]
        no_enable: bool,
        /// Pin to explicit heads (pipe-separated) for doc-id targets
        #[arg(long)]
        heads: Option<String>,
    },
}

pub async fn run(command: PlugsCommands) -> Res<ExitCode> {
    // Boot the drawer first: it attaches to the plugs repo, ensures the core
    // plug, and materializes the cache (the one-shot CLI has no switch).
    lazy::drawer_repo().await?;
    let plugs = lazy::plugs_repo().await?;
    match command {
        PlugsCommands::List => list(&plugs).await,
        PlugsCommands::Show { target } => show(&plugs, &target).await,
        PlugsCommands::Enable { target, heads } => {
            let heads = parse_heads(heads.as_deref())?;
            let ref_url = plugs.enable_target(&target, heads.as_ref()).await?;
            println!("enabled {target} at {ref_url}");
            Ok(ExitCode::SUCCESS)
        }
        PlugsCommands::Disable { plug_id } => {
            let heads = plugs.disable_plug(&plug_id).await?;
            println!(
                "disabled {plug_id} (config heads: {})",
                heads_summary(&heads)
            );
            Ok(ExitCode::SUCCESS)
        }
        PlugsCommands::Update { plug_id } => {
            let heads = plugs.update_plug(&plug_id).await?;
            println!(
                "updated {plug_id} (config heads: {})",
                heads_summary(&heads)
            );
            Ok(ExitCode::SUCCESS)
        }
        PlugsCommands::Pending => pending(&plugs).await,
        PlugsCommands::Import {
            target,
            no_enable,
            heads,
        } => import(&plugs, &target, no_enable, parse_heads(heads.as_deref())?).await,
    }
}

fn parse_heads(heads: Option<&str>) -> Res<Option<ChangeHashSet>> {
    let Some(heads) = heads else {
        return Ok(None);
    };
    let parts: Vec<&str> = heads.split('|').collect();
    Ok(Some(ChangeHashSet(am_utils_rs::parse_commit_heads(
        &parts,
    )?)))
}

fn heads_summary(heads: &ChangeHashSet) -> String {
    am_utils_rs::serialize_commit_heads(&heads.0).join("|")
}

async fn list(plugs: &Arc<PlugsRepo>) -> Res<ExitCode> {
    use comfy_table::Table;
    use comfy_table::presets::NOTHING;

    let config = plugs.get_config().await;
    let known = plugs.list_plugs().await;
    let active = plugs.list_active_plugs().await;

    let active_ids: HashSet<String> = active.iter().map(|plug| plug.id()).collect();
    let enabled: HashSet<String> = config
        .as_ref()
        .map(|cfg| cfg.enabled.keys().cloned().collect())
        .unwrap_or_default();

    let mut ids: Vec<String> = known.iter().map(|plug| plug.id()).collect();
    ids.extend(enabled.iter().cloned());
    ids.sort();
    ids.dedup();

    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["ID", "Version", "Status", "Config Doc"]);
    for id in ids {
        let version = config
            .as_ref()
            .and_then(|cfg| cfg.known_plugs.get(&id))
            .map(|track| track.latest_version.clone())
            .filter(|version| !version.is_empty())
            .or_else(|| {
                known
                    .iter()
                    .find(|plug| plug.id() == id)
                    .map(|plug| plug.version.to_string())
            })
            .unwrap_or_else(|| "-".to_string());
        let status = if enabled.contains(&id) {
            if active_ids.contains(&id) {
                "enabled"
            } else {
                "pending"
            }
        } else {
            "disabled"
        };
        let config_doc = config
            .as_ref()
            .and_then(|cfg| cfg.plug_config_doc_ids.get(&id))
            .cloned()
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![id, version, status.to_string(), config_doc]);
    }
    println!("{table}");
    Ok(ExitCode::SUCCESS)
}

async fn show(plugs: &Arc<PlugsRepo>, target: &str) -> Res<ExitCode> {
    let Some(manifest) = plugs.resolve_manifest(target).await? else {
        eyre::bail!("no manifest for {target} (not known, or not readable at pinned heads)");
    };
    let config = plugs.get_config().await;
    let track = config
        .as_ref()
        .and_then(|cfg| cfg.known_plugs.get(&manifest.id()));
    let enabled = config
        .as_ref()
        .is_some_and(|cfg| cfg.enabled.contains_key(&manifest.id()));

    println!("id: {}", manifest.id());
    println!("version: {}", manifest.version);
    println!("title: {}", manifest.title);
    println!("desc: {}", manifest.desc);
    println!("status: {}", if enabled { "enabled" } else { "disabled" });
    if let Some(track) = track {
        println!("latest: {}", track.latest);
        println!("latest_version: {}", track.latest_version);
        if let Some(reason) = &track.latest_rejection {
            println!("latest_rejection: {reason}");
        }
        println!("last_valid: {}", track.last_valid);
        println!("last_valid_version: {}", track.last_valid_version);
        if let Some(enabled_version) = &track.last_enabled_version {
            println!("last_enabled_version: {enabled_version}");
        }
    }
    if let Some(doc_id) = plugs.get_plug_config_doc_id(&manifest.id()).await {
        println!("config_doc: {doc_id}");
    }
    println!("facets:");
    for facet in &manifest.facets {
        println!("  {}", facet.key_tag);
    }
    println!(
        "dependencies: {}",
        manifest
            .dependencies
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "routines: {}",
        manifest
            .routines
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "commands: {}",
        manifest
            .commands
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(ExitCode::SUCCESS)
}

async fn pending(plugs: &Arc<PlugsRepo>) -> Res<ExitCode> {
    use comfy_table::Table;
    use comfy_table::presets::NOTHING;

    let entries = plugs.list_pending().await;
    if entries.is_empty() {
        println!("no pending plugs");
        return Ok(ExitCode::SUCCESS);
    }
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_header(vec!["ID", "Pinned Ref"]);
    for (id, ref_url) in entries {
        table.add_row(vec![id, ref_url.to_string()]);
    }
    println!("{table}");
    Ok(ExitCode::SUCCESS)
}

async fn import(
    plugs: &Arc<PlugsRepo>,
    target: &str,
    no_enable: bool,
    heads: Option<ChangeHashSet>,
) -> Res<ExitCode> {
    let imported = if target.starts_with("db+facet://") {
        // Facet ref → doc import at the ref's pinned heads.
        let ref_url = url::Url::parse(target)?;
        let parsed = daybook_types::url::parse_facet_ref(&ref_url)?;
        let heads = match parsed.at {
            Some(at_heads) => ChangeHashSet(am_utils_rs::parse_commit_heads(&at_heads)?),
            None => {
                eyre::bail!("facet ref must pin heads for import: {target}");
            }
        };
        plugs
            .import_from_doc_id(&parsed.doc_id, &heads, no_enable)
            .await?
    } else if target.starts_with("oci://") {
        let imported = plugs
            .import_from_oci_registry(
                target,
                oci_client::secrets::RegistryAuth::Anonymous,
                OciImportOptions::default(),
            )
            .await?;
        if !no_enable && let Some(doc_id) = &imported.doc_id {
            plugs.enable_target(doc_id, None).await?;
        }
        imported
    } else if std::path::Path::new(target).exists() {
        // Local OCI layout path.
        let imported = plugs
            .import_from_oci_layout(std::path::Path::new(target), OciImportOptions::default())
            .await?;
        if !no_enable && let Some(doc_id) = &imported.doc_id {
            plugs.enable_target(doc_id, None).await?;
        }
        imported
    } else {
        // Bare doc id → doc import at current main heads (or explicit heads).
        let doc_id = target.to_string();
        plugs
            .import_doc_id(&doc_id, heads.as_ref(), no_enable)
            .await?
    };

    println!(
        "imported {} v{} (doc: {}){}",
        imported.plug_id,
        imported.version,
        imported.doc_id.as_deref().unwrap_or("-"),
        if no_enable { " [known only]" } else { "" }
    );
    Ok(ExitCode::SUCCESS)
}
