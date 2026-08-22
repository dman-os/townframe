use crate::interlude::*;
use std::path::PathBuf;
use std::sync::Arc;

use lettre::AsyncTransport;
use sqlx_utils_rs::SqlCtx;
use wash_plugin_sqlite::SqlPlugin;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::WitInterfaces;
use wash_runtime::wit::{WitInterface, WitWorld};

mod binds_auth {

    wash_runtime::wasmtime::component::bindgen!({
        world: "auth",
        path: "wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        with: {
            "townframe:sqlite/sqlite-connection.connection": wash_plugin_sqlite::SqliteConnectionToken,
            "townframe:sqlite/sqlite-connection.transaction": wash_plugin_sqlite::SqliteTransactionToken,
        }
    });
}

pub use binds_auth::townframe::api_utils::http_service;
pub use binds_auth::townframe::api_utils::mail;

fn wasmtime_err(msg: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(msg.to_string())
}

/// Host plugin for `townframe:api-utils/http-service`.
///
/// Playground implementation, not config-driven yet: resolves a single sqlite
/// connection from a hardcoded file path and hands it to the guest via
/// `get-args`, mirroring how the daybook runtime provides `facet-routine` args.
pub struct ServicePlugin {
    sqlite_file_path: PathBuf,
    sql: tokio::sync::RwLock<Option<SqlCtx>>,
}

impl Default for ServicePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl ServicePlugin {
    pub const ID: &str = "townframe:api-utils/http-service";

    pub fn new() -> Self {
        Self {
            sqlite_file_path: std::env::temp_dir().join("townframe-auth.sqlite"),
            sql: default(),
        }
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    async fn sql_ctx(&self) -> Res<SqlCtx> {
        if let Some(sql) = self.sql.read().await.clone() {
            return Ok(sql);
        }
        let sqlite_url = format!("sqlite://{}", self.sqlite_file_path.display());
        sqlx_utils_rs::init_sqlite_vec();
        let sql = sqlx_utils_rs::SqlCtx::url(&sqlite_url)
            .await
            .wrap_err("error initializing auth sqlite ctx")?;
        let mut slot = self.sql.write().await;
        *slot = Some(sql.clone());
        Ok(sql)
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for ServicePlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:api-utils/http-service",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "api-utils"
                && iface.interfaces.contains("http-service")
            {
                http_service::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                    item.linker(),
                    |ctx| ctx,
                )?;
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        _resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl http_service::Host for SharedWashCtx {
    async fn get_args(&mut self) -> wasmtime::Result<http_service::ServiceArgs> {
        let plugin = ServicePlugin::from_ctx(self);
        let sql = plugin.sql_ctx().await.map_err(wasmtime_err)?;
        let handle = SqlPlugin::create_connection(
            self,
            wash_plugin_sqlite::SqliteConnectionToken {
                sqlite_file_path: plugin.sqlite_file_path.to_string_lossy().to_string(),
                sql,
            },
        )?;
        Ok(http_service::ServiceArgs {
            sqlite_connections: vec![("auth-db".to_string(), handle)],
        })
    }
}

/// Host plugin for `townframe:api-utils/mail`.
///
/// Native SMTP transport (lettre) for the btress auth service. Config comes
/// from env vars (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`,
/// `SMTP_TLS`, `MAIL_FROM`); the transport is built lazily and cached.
pub struct MailPlugin {
    smtp: tokio::sync::RwLock<Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>>,
    from: String,
}

impl Default for MailPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl MailPlugin {
    pub const ID: &str = "townframe:api-utils/mail";

    pub fn new() -> Self {
        Self {
            smtp: default(),
            from: std::env::var("MAIL_FROM").unwrap_or_else(|_| "btress@localhost".into()),
        }
    }

    fn from_ctx(wcx: &SharedWashCtx) -> Arc<Self> {
        wcx.active_ctx.get_plugin::<Self>(Self::ID)
    }

    async fn transport(&self) -> Res<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>> {
        if let Some(smtp) = self.smtp.read().await.clone() {
            return Ok(smtp);
        }
        let host = std::env::var("SMTP_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("SMTP_PORT")
            .ok()
            .and_then(|port| port.parse().ok())
            .unwrap_or(2500);
        let user = std::env::var("SMTP_USER").unwrap_or_default();
        let pass = std::env::var("SMTP_PASS").unwrap_or_default();
        let tls = std::env::var("SMTP_TLS").unwrap_or_else(|_| "plain".into());

        let mut builder = match tls.as_str() {
            "starttls" => {
                lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::starttls_relay(&host)?
            }
            "tls" => lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(&host)?,
            _ => lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::builder_dangerous(&host),
        };
        builder = builder.port(port);
        if !user.is_empty() {
            builder = builder.credentials(
                lettre::transport::smtp::authentication::Credentials::new(user, pass),
            );
        }
        let smtp = builder.build();
        let mut slot = self.smtp.write().await;
        *slot = Some(smtp.clone());
        Ok(smtp)
    }

    async fn send(&self, message: mail::EmailMessage) -> Res<()> {
        let transport = self.transport().await?;
        let from = message
            .from_address
            .clone()
            .unwrap_or_else(|| self.from.clone());
        let mut builder = lettre::Message::builder()
            .from(from.parse()?)
            .to(message.to.parse()?)
            .subject(message.subject.clone())
            .header(lettre::message::header::ContentType::TEXT_HTML);
        if let Some(reply_to) = &message.reply_to {
            builder = builder.reply_to(reply_to.parse()?);
        }
        let email = builder.body(message.html)?;
        transport.send(email).await?;
        Ok(())
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for MailPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:api-utils/mail",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "api-utils"
                && iface.interfaces.contains("mail")
            {
                mail::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                    item.linker(),
                    |ctx| ctx,
                )?;
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        _resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl mail::Host for SharedWashCtx {
    async fn send(
        &mut self,
        message: mail::EmailMessage,
    ) -> wasmtime::Result<Result<(), mail::MailError>> {
        let plugin = MailPlugin::from_ctx(self);
        match plugin.send(message).await {
            Ok(()) => Ok(Ok(())),
            Err(err) => Ok(Err(mail::MailError::SendFailed(err.to_string()))),
        }
    }
}
