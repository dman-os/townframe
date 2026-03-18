/*
Lifted from Samod
The MIT License (MIT)

Copyright © 2025 Alex Good
 */

#![allow(dead_code)]
use std::{
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};

use samod::{transport::channel::ChannelDialer, AcceptorHandle, BackoffConfig, DialerHandle, Repo};
use url::Url;

static NEXT_TINCAN_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct Connected {
    dialer_handle: DialerHandle,
    acceptor_handle: AcceptorHandle,
}

impl Connected {
    pub async fn disconnect(self) {
        self.dialer_handle.close();
        self.acceptor_handle.close();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(crate) fn connect_repos(left: &Repo, right: &Repo) -> Connected {
    let id = NEXT_TINCAN_ID.fetch_add(1, Ordering::Relaxed);
    let url = Url::parse(&format!("channel://tincans-{id}")).expect("valid tincans url");
    let acceptor_handle = right.make_acceptor(url).expect("failed to create acceptor");
    let dialer = ChannelDialer::new(acceptor_handle.clone());
    let dialer_handle = left
        .dial(BackoffConfig::default(), Arc::new(dialer))
        .expect("failed to dial acceptor");

    Connected {
        dialer_handle,
        acceptor_handle,
    }
}
