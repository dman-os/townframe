use iroh_tickets::endpoint::EndpointTicket;

use crate::interlude::*;

struct IrohSyncRepo {
    acx: AmCtx,
    router: iroh::protocol::Router,
}

impl IrohSyncRepo {
    // FIXME: add a stop token
    pub async fn boot(acx: AmCtx, config_repo: &crate::config::ConfigRepo) -> Res<Arc<Self>> {
        // TODO: get from config repo
        // TODO: `cargo add keyring` crate and store some data in keyring
        // in config repo
        let sec_key = iroh::SecretKey::generate(&mut rand::rng());
        let pub_key = sec_key.public();

        let endpoint = iroh::Endpoint::builder().secret_key(sec_key).bind().await?;
        let router = iroh::protocol::Router::builder(endpoint.clone())
            // FIXME: instead of implementing the Protocol trait on AmCtx
            // in am_utils_rs::iroh, let's make a warapper that allows reading who is connected. the wrapper should go in am_utils_rs::iroh
            .accept(AmCtx::SYNC_ALPN, acx.clone())
            .spawn();

        // TODO: stop token for shutting down the router
        router.shutdown();

        Ok(Self { acx, router }.into())
    }

    pub async fn spawn_auto_connect(&self) {
        // TODO: spawn a tokio task and return an encapsulateed handle stop token
        // using join handle and cancellation token
        // the auto sync will iterate known devices and try to connect
        // to them peirodically
        // TODO: we should avoid trying to connect to devices we're already connected
        // too through an incoming connection (from the router above)
        // TODO: this is going to be a select! loop
        // to avoid spamming, we'll use a timer in-between attempts
        // the retuerned handles hould support try_reconnect that will
        // allow us to provide a ui butto for "try now" or when conditions
        // are detected to be different
        // TODO: actually, let's separate the AutoSyncHandle and it's stop
        // token into two types as seen elsewhere in the repos.
        // let's return an Arced handle.
        // TODO: additionally, we'll want to make sure we have one auto sync per Repo
        // TODO: we should also llisten to the config repo in our auto sync so that
        // we can observe changes to known devices. we'll need to add events to ConfigRepo
    }

    pub async fn connect_device(&self, ticket: EndpointTicket) {
        // FIXME: add seen devices to ConfigRepo
        // - NOTE: we'll not store it in the automerge backed Store
        // - let's use...idk, globals?
        let addr = ticket.endpoint_addr();
        let conn = self
            .acx
            .spawn_connection_iroh(self.router.endpoint(), addr)
            .await?;
        // TODO: we don't store here automatically to a config repo
        // we want that to be an explicit step
    }

    pub async fn get_addr(&self) -> EndpointTicket {
        EndpointTicket::new(self.router.endpoint().addr())
    }

    pub async fn get_ticket(&self) -> EndpointTicket {
        EndpointTicket::new(self.router.endpoint().addr())
    }
}
