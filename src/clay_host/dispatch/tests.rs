use super::*;
use std::sync::Mutex;

/// Records every call it receives, with the identity it was made as, and answers 200.
#[derive(Default)]
struct Recording(Mutex<Vec<(MicroId, DispatchCall)>>);

impl MicroDispatch for Recording {
    fn call(&self, micro: &MicroId, call: DispatchCall) -> BoxFuture<'static, DispatchAnswer> {
        self.0.lock().unwrap().push((micro.clone(), call));
        Box::pin(async {
            DispatchAnswer {
                status: 200,
                headers: Vec::new(),
                body: b"{}".to_vec(),
            }
        })
    }
}

fn outgoing(method: &str, authority: &str, path: &str) -> Outgoing {
    Outgoing {
        method: method.to_owned(),
        scheme: Some("http".to_owned()),
        authority: Some(authority.to_owned()),
        path_with_query: Some(path.to_owned()),
        headers: Vec::new(),
        body: Vec::new(),
    }
}

fn egress() -> (MicroEgress, Arc<Recording>) {
    let recording = Arc::new(Recording::default());
    (
        MicroEgress::new(MicroId::new("did:example:alice/counter"), recording.clone()),
        recording,
    )
}

#[tokio::test]
async fn an_admitted_call_carries_the_egress_identity_and_never_a_header_one() {
    let (egress, recording) = egress();
    let mut request = outgoing("POST", DISPATCH_AUTHORITY, "/xrpc/test.echo");
    request.headers = vec![
        (
            "x-micro-id".to_owned(),
            b"did:example:mallory/other".to_vec(),
        ),
        ("authorization".to_owned(), b"Bearer stolen".to_vec()),
    ];
    request.body = b"{\"n\":1}".to_vec();
    let answer = egress
        .send(request)
        .await
        .expect("dispatch.local is admitted");
    assert_eq!(answer.status, 200);
    let calls = recording.0.lock().unwrap();
    let [(micro, call)] = calls.as_slice() else {
        panic!("one call expected, got {calls:?}");
    };
    assert_eq!(micro.as_str(), "did:example:alice/counter");
    assert_eq!(call.method, DispatchMethod::Post);
    assert_eq!(call.nsid, "test.echo");
    assert_eq!(call.body, b"{\"n\":1}");
    assert!(call.headers.iter().any(|(name, _)| name == "x-micro-id"));
}

#[tokio::test]
async fn a_get_carries_its_raw_query() {
    let (egress, recording) = egress();
    egress
        .send(outgoing(
            "GET",
            DISPATCH_AUTHORITY,
            "/xrpc/test.ping?a=1&b=%20",
        ))
        .await
        .unwrap();
    let calls = recording.0.lock().unwrap();
    assert_eq!(calls[0].1.method, DispatchMethod::Get);
    assert_eq!(calls[0].1.nsid, "test.ping");
    assert_eq!(calls[0].1.query, "a=1&b=%20");
}

#[tokio::test]
async fn every_other_origin_is_denied_before_dispatch_hears_of_it() {
    let (egress, recording) = egress();
    for (scheme, authority) in [
        ("http", "example.com"),
        ("https", DISPATCH_AUTHORITY),
        ("http", "dispatch.local:8080"),
        ("http", "dispatch.local.evil.test"),
        ("http", "DISPATCH.LOCAL"),
        ("http", "user@dispatch.local"),
    ] {
        let mut request = outgoing("POST", authority, "/xrpc/test.echo");
        request.scheme = Some(scheme.to_owned());
        assert_eq!(
            egress.send(request).await.unwrap_err(),
            Denied,
            "{scheme}://{authority}"
        );
    }
    let mut no_authority = outgoing("POST", DISPATCH_AUTHORITY, "/xrpc/test.echo");
    no_authority.authority = None;
    assert_eq!(egress.send(no_authority).await.unwrap_err(), Denied);
    assert!(recording.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn other_verbs_and_paths_are_answered_not_forwarded() {
    let (egress, recording) = egress();
    let cases = [
        ("DELETE", "/xrpc/test.echo", 405),
        ("PUT", "/xrpc/test.echo", 405),
        ("POST", "/other", 404),
        ("POST", "/", 404),
        ("POST", "/xrpc", 404),
        ("POST", "/xrpc/", 400),
        ("POST", "/xrpc/a/b", 400),
        ("POST", "/xrpc/a%2Fb", 400),
        ("POST", "/xrpc/test.echo?x=1", 400),
    ];
    for (method, path, status) in cases {
        let answer = egress
            .send(outgoing(method, DISPATCH_AUTHORITY, path))
            .await
            .unwrap();
        assert_eq!(answer.status, status, "{method} {path}");
        assert!(answer.body.starts_with(b"{\"error\":"), "{method} {path}");
    }
    let mut get_with_body = outgoing("GET", DISPATCH_AUTHORITY, "/xrpc/test.ping");
    get_with_body.body = b"{}".to_vec();
    assert_eq!(egress.send(get_with_body).await.unwrap().status, 400);
    assert!(recording.0.lock().unwrap().is_empty());
}
