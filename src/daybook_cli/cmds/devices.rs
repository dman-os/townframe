use crate::interlude::*;

#[derive(Debug, clap::Subcommand)]
pub enum DevicesCommands {
    /// List known devices
    Ls,
    /// Add a device from a bootstrap URL
    Add {
        /// Clone URL: db+iroh-clone:<endpoint-ticket>
        iroh_ticket_url: String,
        /// Override display name
        #[arg(long)]
        name: Option<String>,
    },
}

pub async fn run(command: DevicesCommands) -> Res<ExitCode> {
    let cx = lazy::repo_ctx().await?;
    let config_repo = crate::lazy::config_repo().await?;

    match command {
        DevicesCommands::Ls => {
            use comfy_table::presets::NOTHING;
            use comfy_table::Table;

            let mut devices = config_repo.list_known_sync_devices().await?;
            devices.sort_by_key(|device| device.added_at);

            let mut table = Table::new();
            table
                .load_preset(NOTHING)
                .set_header(vec!["Endpoint", "Name", "Added At"]);
            for device in devices {
                table.add_row(vec![
                    utils_rs::hash::encode_base58_multibase(device.endpoint_id),
                    device.name,
                    device.added_at.to_string(),
                ]);
            }
            println!("{table}");
        }
        DevicesCommands::Add {
            iroh_ticket_url,
            name,
        } => {
            let provision = daybook_core::sync::request_clone_provision_from_url(
                &iroh_ticket_url,
                daybook_core::sync::RequestCloneProvisionReq {
                    requested_device_name: None,
                    requester_endpoint_id: cx.iroh_public_key.clone(),
                    requester_contact_card: cx.big_repo.local_keyhive_contact_card(),
                },
            )
            .await?;
            let bootstrap = provision.to_bootstrap_state()?;
            let local_repo_id = cx.repo_id.clone();
            if bootstrap.repo_id != local_repo_id {
                eyre::bail!(
                    "ticket repo_id mismatch (local={}, remote={})",
                    local_repo_id,
                    bootstrap.repo_id
                );
            }
            let device_name = if let Some(name) = name {
                name.clone()
            } else if let Some(name) = bootstrap.device_name {
                name
            } else {
                bootstrap.endpoint_id.to_string()
            };
            config_repo
                .upsert_known_sync_device(daybook_core::repo::globals::SyncDeviceEntry {
                    endpoint_id: bootstrap.endpoint_id,
                    agent_peer_id: None,
                    name: device_name,
                    added_at: Timestamp::now(),
                    last_connected_at: None,
                })
                .await?;
        }
    }
    Ok(ExitCode::SUCCESS)
}
