use crate::interlude::*;
use crate::out::out;

/// Sets up a new node directory: the directory itself and `node.json` with a fresh identity.
/// Nothing is booted; the node's databases are created by its first boot.
///
/// Idempotent: a directory that already holds a valid node is reported and left exactly as it
/// is, so running this twice cannot change a node's identity. A directory that holds something
/// that is not a node (an unreadable or unparsable `node.json`, a version this `day` does not
/// understand) is an error, and is not touched either.
pub async fn run(dir: Option<PathBuf>, relay: bool, as_json: bool) -> Res<ExitCode> {
    let mut out = out();
    let node_dir = match dir {
        Some(dir) => {
            day_node::NodeDir::new(crate::config::CliConfig::source(Some(&dir))?.node_path)
        }
        None => lazy::config().await?.node_dir.clone(),
    };
    if node_dir.is_initialized() {
        // `settings` is the validity check: it parses the file and its version.
        let settings = node_dir.settings().wrap_err_with(|| {
            format!(
                "{} exists but is not a valid node; refusing to touch it",
                node_dir.root().display()
            )
        })?;
        if as_json {
            report_json(&mut out, &settings.node_id(), node_dir.root(), false)?;
        } else {
            writeln!(
                out,
                "node {} already initialized at {}",
                settings.node_id(),
                node_dir.root().display()
            )?;
        }
        return Ok(ExitCode::SUCCESS);
    }
    node_dir.init().await?;
    if relay {
        node_dir
            .update_settings(|settings| {
                settings.relay = true;
                settings.peers.get_or_insert_with(default);
                Ok(())
            })
            .await?;
    }
    let settings = node_dir.settings()?;
    if as_json {
        report_json(&mut out, &settings.node_id(), node_dir.root(), true)?;
    } else {
        writeln!(
            out,
            "initialized node {} at {}",
            settings.node_id(),
            node_dir.root().display()
        )?;
    }
    Ok(ExitCode::SUCCESS)
}

/// `created` is false when the directory already held a node, which `init` leaves as it is.
fn report_json(out: &mut crate::out::Out, node_id: &str, path: &Path, created: bool) -> Res<()> {
    let report = json!({ "nodeId": node_id, "path": path, "created": created });
    writeln!(out, "{}", serde_json::to_string_pretty(&report)?)?;
    Ok(())
}
