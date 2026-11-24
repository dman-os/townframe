//! `materialize`: the guest's way to get a blob as a file in its working mount (ADR 051 §2).
//!
//! A guest calls it the way it calls everything else the host serves, as an XRPC request to
//! `dispatch.local`: `POST /xrpc/org.example.daybook.blobs.materialize` with a JSON body naming
//! the blobs handle and the blob, either by digest (`{ "capability", "blobId" }`) or as the
//! `Blob` datum that carries it (`{ "capability", "blob" }`, whose `digest` is used). It answers
//! `{ "path", "info" }`, where `path` is a file under [`WORKING_MOUNT`] the guest opens with
//! ordinary file I/O.
//!
//! It is served here, by [`Egress`], and does not go through dispatch. The destination is the
//! calling workload's own mount, which only the executor knows (it is found by the host's workload
//! id, never anything the guest says), and the authorization is the blobs handle alone: the same
//! check a `blobs.read` of that blob makes (`BlobService::materialize`), under the workload's own
//! actor as the presenter, so a handle bound to another actor is refused.

use crate::Egress;
use crate::WORKING_MOUNT;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::BlobsError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_nodule::XrpcBody;
use hyper::HeaderMap;
use hyper::StatusCode;
use method_core::ContextError;
use serde::Deserialize;
use serde_json::json;

/// The method's name in a guest's request path.
pub(crate) const NSID: &str = "org.example.daybook.blobs.materialize";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    capability: CapId,
    blob_id: Option<BlobId>,
    blob: Option<Blob>,
}

fn answer(status: StatusCode, error: &str, message: &str) -> (StatusCode, XrpcBody) {
    (
        status,
        XrpcBody::json(
            serde_json::to_vec(&json!({ "error": error, "message": message }))
                .expect("a JSON value serializes"),
        ),
    )
}

impl Egress {
    pub(crate) async fn materialize(
        &self,
        workload_id: &str,
        headers: &HeaderMap,
        body: XrpcBody,
    ) -> (StatusCode, XrpcBody) {
        let XrpcBody::Json(body) = body else {
            return answer(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "materialize takes a JSON body",
            );
        };
        let request: Request = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(error) => {
                return answer(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    &format!("the request is not `{{ capability, blobId }}` or `{{ capability, blob }}`: {error}"),
                );
            }
        };
        let blob_id = match (request.blob_id, request.blob) {
            (Some(blob_id), None) => blob_id,
            (None, Some(blob)) => match blob.digest.parse() {
                Ok(blob_id) => blob_id,
                Err(error) => {
                    return answer(
                        StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        &format!("the blob's digest is not a blob id: {error}"),
                    );
                }
            },
            _ => {
                return answer(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "name the blob by exactly one of `blobId` and `blob`",
                );
            }
        };
        let (context, _) = match service_core_rs::xrpc::request_context(headers) {
            Ok(parsed) => parsed,
            Err(message) => return answer(StatusCode::BAD_REQUEST, "InvalidRequest", &message),
        };
        let signer = self
            .signers
            .lock()
            .expect("signers lock")
            .get(workload_id)
            .map(|caller| caller.signer);
        let mount = self
            .mounts
            .lock()
            .expect("mounts lock")
            .get(workload_id)
            .cloned();
        // Stopped while the request was still running, as in `big_nodule::egress::call`.
        let (Some(signer), Some(mount)) = (signer, mount) else {
            return answer(
                StatusCode::SERVICE_UNAVAILABLE,
                "WorkloadStopped",
                "the calling handler has been stopped",
            );
        };
        let authorization = Authorization {
            authority: request.capability,
            signer,
        };
        match self
            .blobs
            .materialize(&context.next_hop(), authorization, blob_id, &mount)
            .await
        {
            Ok(materialized) => (
                StatusCode::OK,
                XrpcBody::json(
                    serde_json::to_vec(&json!({
                        "path": format!("{WORKING_MOUNT}/{}", materialized.name),
                        "info": materialized.info,
                    }))
                    .expect("a JSON value serializes"),
                ),
            ),
            Err(error @ BlobsError::Denied) => {
                answer(StatusCode::FORBIDDEN, "Denied", &error.to_string())
            }
            Err(BlobsError::Context(ContextError::DeadlineExceeded)) => answer(
                StatusCode::GATEWAY_TIMEOUT,
                "DeadlineExceeded",
                "the blob did not arrive before the call's deadline",
            ),
            Err(BlobsError::Context(error)) => {
                answer(StatusCode::BAD_REQUEST, "InvalidRequest", &error.to_string())
            }
            Err(error) => answer(
                StatusCode::BAD_GATEWAY,
                "BadGateway",
                &error.to_string(),
            ),
        }
    }
}
