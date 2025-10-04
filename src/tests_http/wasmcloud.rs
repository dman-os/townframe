use crate::interlude::*;

pub struct TestApp {
    pub client: wadm_client::Client,
    pub app_url: url::Url,
    pub app_name: String,
}

impl TestApp {
    pub async fn new(test_name: &'static str, wasm_path: &'static str) -> Res<Self> {
        let app_name = test_name.replace("::tests::", "_").replace("::", "_");

        let host =
            utils_rs::get_env_var("WASMCLOUD_CTL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port =
            utils_rs::get_env_var("WASMCLOUD_CTL_PORT").unwrap_or_else(|_| "4222".to_string());
        let nats_url = format!("nats://{host}:{port}");
        let lattice =
            utils_rs::get_env_var("WASMCLOUD_LATTICE").unwrap_or_else(|_| "default".to_string());

        let opts = wadm_client::ClientConnectOptions {
            url: Some(nats_url),
            ..Default::default()
        };
        let client = wadm_client::Client::new(&lattice, None, opts)
            .await
            .map_err(utils_rs::anyhow_to_eyre!())?;

        // Use ephemeral-ish port range to reduce collisions: 18000-20000

        use rand::Rng;
        let http_port = rand::rng().random_range(20000..40000);
        let manifest = make_inline_manifest(
            &app_name,
            http_port,
            &format!(
                "file://{root}/../../{wasm_path}",
                root = env!("CARGO_MANIFEST_DIR")
            ),
        );

        println!("manifest: {manifest:?}");
        client.delete_manifest(&app_name, None).await?;
        client.put_and_deploy_manifest(manifest).await?;

        loop {
            let status = client.get_manifest_status(&app_name).await?;
            match status.info.status_type {
                wadm_types::api::StatusType::Undeployed
                | wadm_types::api::StatusType::Waiting
                | wadm_types::api::StatusType::Reconciling => {}
                wadm_types::api::StatusType::Deployed => break,
                wadm_types::api::StatusType::Failed | wadm_types::api::StatusType::Unhealthy => {
                    eyre::bail!("unexpected status: {status:?}")
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let app_url = url::Url::parse(&format!("http://127.0.0.1:{http_port}/")).unwrap();

        Ok(Self {
            client,
            app_name,
            app_url,
        })
    }

    pub async fn close(self) -> Res<()> {
        self.client.delete_manifest(&self.app_name, None).await?;
        Ok(())
    }
}

fn make_inline_manifest(app_name: &str, http_port: u16, image: &str) -> wadm_types::Manifest {
    use wadm_types::*;

    Manifest {
        api_version: "core.oam.dev/v1beta1".to_string(),
        kind: "Application".to_string(),
        metadata: Metadata {
            labels: default(),
            name: app_name.to_string(),
            annotations: [("version".to_string(), "v0.0.1".to_string())]
                .into_iter()
                .collect(),
        },
        spec: Specification {
            policies: default(),
            components: vec![
                Component {
                    name: "http-component".to_string(),
                    properties: Properties::Component {
                        properties: ComponentProperties {
                            image: Some(image.to_string()),
                            application: None,
                            id: None,
                            config: vec![],
                            secrets: vec![],
                        },
                    },
                    traits: Some(vec![Trait {
                        trait_type: "spreadscaler".to_string(),
                        properties: TraitProperty::SpreadScaler(SpreadScalerProperty {
                            instances: 1,
                            spread: vec![],
                        }),
                    }]),
                },
                Component {
                    name: "httpserver".to_string(),
                    properties: Properties::Capability {
                        properties: CapabilityProperties {
                            image: Some("ghcr.io/wasmcloud/http-server:0.27.0".to_string()),
                            application: None,
                            id: None,
                            config: vec![],
                            secrets: vec![],
                        },
                    },
                    traits: Some(vec![Trait {
                        trait_type: "link".to_string(),
                        properties: TraitProperty::Link(LinkProperty {
                            target: TargetConfig {
                                name: "http-component".to_string(),
                                config: vec![],
                                secrets: vec![],
                            },
                            namespace: "wasi".to_string(),
                            package: "http".to_string(),
                            interfaces: vec!["incoming-handler".to_string()],
                            source: Some(ConfigDefinition {
                                config: vec![ConfigProperty {
                                    name: "default-http".to_string(),
                                    properties: Some(
                                        [(
                                            "address".to_string(),
                                            format!("127.0.0.1:{http_port}").to_string(),
                                        )]
                                        .into_iter()
                                        .collect(),
                                    ),
                                }],
                                secrets: vec![],
                            }),
                            name: None,
                            ..default()
                        }),
                    }]),
                },
            ],
        },
    }
}
