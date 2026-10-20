//! Snapshots pinning the serde wire form of every public DTO in [`super::protocol`], so a wire
//! change shows up as a reviewable snapshot diff. Fixtures are deterministic byte patterns.

use super::*;
use crate::authority::{AuthorityServiceError, CapId};
use big_cabinet_core::dmeta::{
    Bytes, ClaimOperation, FacetClaim, FacetSignature, MultibaseKey, SignatureBytes, Uuid7, cid_of,
};
use service_utils_rs::ContextError;

fn hash(byte: u8) -> ChangeHash {
    ChangeHash([byte; 32])
}

fn document(byte: u8) -> DocumentId {
    DocumentId::new([byte; 32])
}

fn capability(byte: u8) -> CapId {
    serde_json::from_value(json!(uuid::Uuid::from_bytes([byte; 16]))).unwrap()
}

fn uuid7(byte: u8) -> Uuid7 {
    let mut bytes = [byte; 16];
    bytes[6] = 0x70 | (bytes[6] & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    Uuid7::try_from(uuid::Uuid::from_bytes(bytes)).unwrap()
}

fn command(text: &str) -> CommandId {
    CommandId::try_from(text.to_owned()).unwrap()
}

fn claim() -> FacetClaim {
    FacetClaim {
        origin_document: document(1),
        facet_key: key("main"),
        chosen_uuid: uuid7(1),
        actor_uuid: uuid7(2),
        operation: ClaimOperation::Set,
        value_cid: Some(cid_of(b"value")),
        predecessors: vec![cid_of(b"predecessor")],
    }
}

fn verified_evidence() -> SignatureEvidence {
    SignatureEvidence::VerifiedSignature {
        evidence: Box::new(VerifiedSignatureEvidence {
            signature: FacetSignature {
                actor_uuid: uuid7(2),
                doc_uuid: uuid7(3),
                signature: SignatureBytes::try_from(&[7u8; 64][..]).unwrap(),
                message: None,
                inherited: None,
            },
            actor_public_key: MultibaseKey::try_from(&[8u8; 32][..]).unwrap(),
            claim: claim(),
            introduction_marker: "marker-1".into(),
            introduction_hash: hash(5),
            parents: vec![hash(6)],
            resulting_heads: vec![hash(7)],
            automerge_actor: Bytes::from(vec![1, 2, 3]),
        }),
    }
}

fn claims_inspection() -> FacetClaimsInspection {
    FacetClaimsInspection {
        document_id: document(1),
        facet_key: key("main"),
        heads: vec![hash(7)],
        scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
        evidence: vec![
            FacetClaimEvidence {
                marker: "marker-1".into(),
                result: verified_evidence(),
            },
            FacetClaimEvidence {
                marker: "marker-2".into(),
                result: SignatureEvidence::Pending {
                    reason: "verification could not complete".into(),
                },
            },
        ],
    }
}

#[test]
fn command_id_wire() {
    insta::assert_json_snapshot!(command("create-note-1"));
}

#[test]
fn at_wire() {
    for (name, at) in [
        ("at_current", At::Current),
        ("at_heads", At::Heads(vec![hash(1), hash(2)])),
    ] {
        insta::assert_json_snapshot!(name, at);
    }
}

#[test]
fn write_mode_wire() {
    for (name, mode) in [
        (
            "write_mode_if_document_unchanged",
            WriteMode::IfDocumentUnchanged {
                heads: vec![hash(1)],
            },
        ),
        (
            "write_mode_if_facets_unchanged",
            WriteMode::IfFacetsUnchanged {
                facets: BTreeMap::from([(key("main"), vec![hash(2)])]),
            },
        ),
        (
            "write_mode_at_heads",
            WriteMode::AtHeads {
                heads: vec![hash(3)],
            },
        ),
        ("write_mode_at_current", WriteMode::AtCurrent),
    ] {
        insta::assert_json_snapshot!(name, mode);
    }
}

#[test]
fn facet_read_wire() {
    for (name, read) in [
        (
            "facet_read_accepted",
            FacetRead::Accepted {
                value: note("hello"),
                facet_heads: vec![hash(1)],
            },
        ),
        (
            "facet_read_absent_written",
            FacetRead::Absent {
                facet_heads: vec![hash(2)],
            },
        ),
        (
            "facet_read_absent_never_written",
            FacetRead::Absent {
                facet_heads: vec![],
            },
        ),
        (
            "facet_read_pending",
            FacetRead::Pending {
                reason: "verification could not complete".into(),
                facet_heads: vec![hash(3)],
                value: None,
            },
        ),
        (
            "facet_read_pending_optimistic",
            FacetRead::Pending {
                reason: "verification could not complete".into(),
                facet_heads: vec![hash(3)],
                value: Some(note("unverified")),
            },
        ),
        (
            "facet_read_rejected",
            FacetRead::Rejected {
                reason: "unsigned value".into(),
            },
        ),
        (
            "facet_read_undecodable",
            FacetRead::Undecodable {
                reason: "not a string".into(),
                facet_heads: vec![hash(4)],
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, read);
    }
}

#[test]
fn facets_read_wire() {
    insta::assert_json_snapshot!(FacetsRead {
        heads: vec![hash(1)],
        facets: BTreeMap::from([
            (
                key("main"),
                FacetRead::Accepted {
                    value: note("hello"),
                    facet_heads: vec![hash(2)],
                },
            ),
            (
                key("extra"),
                FacetRead::Absent {
                    facet_heads: vec![],
                }
            ),
        ]),
    });
}

#[test]
fn facet_listing_wire() {
    insta::assert_json_snapshot!(FacetListing {
        heads: vec![hash(1)],
        facets: BTreeMap::from([(key("main"), vec![hash(2), hash(3)])]),
    });
}

#[test]
fn write_receipt_wire() {
    insta::assert_json_snapshot!(WriteReceipt {
        document_id: document(1),
        heads: vec![hash(1), hash(2)],
    });
}

#[test]
fn unaccepted_wire() {
    for (name, unaccepted) in [
        (
            "unaccepted_pending",
            Unaccepted::Pending {
                reason: "verification could not complete".into(),
            },
        ),
        (
            "unaccepted_rejected",
            Unaccepted::Rejected {
                reason: "unsigned value".into(),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, unaccepted);
    }
}

#[test]
fn documents_error_wire() {
    for (name, error) in [
        (
            "error_context",
            DocumentsError::Context(ContextError::Cancelled),
        ),
        ("error_denied", DocumentsError::Denied),
        (
            "error_authority",
            DocumentsError::Authority(AuthorityServiceError::InvalidCapability),
        ),
        (
            "error_authority_unavailable",
            DocumentsError::AuthorityUnavailable("connection refused".into()),
        ),
        (
            "error_access",
            DocumentsError::Access("placement denied".into()),
        ),
        (
            "error_repository",
            DocumentsError::Repository("disk full".into()),
        ),
        ("error_document_not_found", DocumentsError::DocumentNotFound),
        (
            "error_unknown_heads",
            DocumentsError::UnknownHeads {
                heads: vec![hash(9)],
            },
        ),
        ("error_invalid_command", DocumentsError::InvalidCommand),
        (
            "error_command_in_flight",
            DocumentsError::CommandInFlight {
                command: command("cmd-1"),
            },
        ),
        (
            "error_command_reused",
            DocumentsError::CommandReused {
                command: command("cmd-1"),
            },
        ),
        (
            "error_unknown_schema",
            DocumentsError::UnknownSchema {
                facet_tag: NOTE.into(),
            },
        ),
        (
            "error_system_facet",
            DocumentsError::SystemFacet {
                facet_key: key("main"),
            },
        ),
        (
            "error_set_and_removed",
            DocumentsError::SetAndRemoved {
                facet_key: key("main"),
            },
        ),
        (
            "error_invalid_value",
            DocumentsError::InvalidValue {
                facet_key: key("main"),
                reason: "not a string".into(),
            },
        ),
        (
            "error_conflict",
            DocumentsError::Conflict {
                heads: vec![hash(3)],
                facets: BTreeMap::from([(key("main"), vec![hash(4)])]),
            },
        ),
        (
            "error_base_not_accepted",
            DocumentsError::BaseNotAccepted {
                facet_key: key("main"),
                verdict: Unaccepted::Rejected {
                    reason: "unsigned value".into(),
                },
            },
        ),
        (
            "error_pending",
            DocumentsError::Pending {
                reason: "inspection evidence pending".into(),
            },
        ),
        (
            "error_resource_refused",
            DocumentsError::ResourceRefused {
                reason: "budget exhausted".into(),
            },
        ),
        (
            "error_unsupported",
            DocumentsError::Unsupported {
                reason: "claim v3".into(),
            },
        ),
        (
            "error_malformed_evidence",
            DocumentsError::MalformedEvidence {
                reason: "truncated signature".into(),
            },
        ),
        ("error_unrelated_history", DocumentsError::UnrelatedHistory),
    ] {
        insta::assert_json_snapshot!(name, error);
    }
}

#[test]
fn create_document_wire() {
    insta::assert_json_snapshot!(CreateDocument {
        capability: capability(1),
        command: command("create-note-1"),
        placement: PLACEMENT.into(),
        facets: facet_map(&[(&key("main"), note("hello"))]),
    });
}

#[test]
fn branch_document_wire() {
    for (name, request) in [
        (
            "branch_document_announced",
            BranchDocument {
                capability: capability(1),
                command: command("branch-1"),
                document_id: document(1),
                at: At::Heads(vec![hash(1)]),
                path: "/draft".into(),
                stealth: false,
                placement: PLACEMENT.into(),
            },
        ),
        (
            "branch_document_stealth",
            BranchDocument {
                capability: capability(1),
                command: command("branch-2"),
                document_id: document(1),
                at: At::Current,
                path: "/private".into(),
                stealth: true,
                placement: PLACEMENT.into(),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, request);
    }
}

#[test]
fn merge_document_wire() {
    for (name, request) in [
        (
            "merge_document_with_command",
            MergeDocument {
                capability: capability(1),
                command: Some(command("merge-1")),
                source: document(2),
                at: At::Heads(vec![hash(2)]),
                target: document(1),
            },
        ),
        (
            "merge_document_bare",
            MergeDocument {
                capability: capability(1),
                command: None,
                source: document(2),
                at: At::Current,
                target: document(1),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, request);
    }
}

#[test]
fn write_facets_wire() {
    for (name, request) in [
        (
            "write_facets_with_command",
            WriteFacets {
                capability: capability(1),
                command: Some(command("write-1")),
                document_id: document(1),
                mode: WriteMode::IfDocumentUnchanged {
                    heads: vec![hash(1)],
                },
                set: facet_map(&[(&key("main"), note("hello"))]),
                remove: BTreeSet::from([key("extra")]),
            },
        ),
        (
            "write_facets_bare",
            WriteFacets {
                capability: capability(1),
                command: None,
                document_id: document(1),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::new(),
                remove: BTreeSet::new(),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, request);
    }
}

#[test]
fn read_facets_wire() {
    for (name, request) in [
        (
            "read_facets_all",
            ReadFacets {
                capability: capability(1),
                document_id: document(1),
                at: At::Current,
                facets: None,
                optimistic: false,
            },
        ),
        (
            "read_facets_named_optimistic",
            ReadFacets {
                capability: capability(1),
                document_id: document(1),
                at: At::Heads(vec![hash(1)]),
                facets: Some(BTreeSet::from([key("main")])),
                optimistic: true,
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, request);
    }
}

#[test]
fn list_facets_wire() {
    insta::assert_json_snapshot!(ListFacets {
        capability: capability(1),
        document_id: document(1),
        at: At::Heads(vec![hash(1)]),
    });
}

#[test]
fn unsafe_inspect_facet_wire() {
    insta::assert_json_snapshot!(UnsafeInspectFacet {
        capability: capability(1),
        document_id: document(1),
        facet_key: key("main"),
        heads: vec![hash(1)],
    });
    insta::assert_json_snapshot!(
        "unsafe_facet_inspection",
        UnsafeFacetInspection {
            native_value: Some(Bytes::from(vec![4, 5, 6])),
            schema_known: true,
            claims: claims_inspection(),
        }
    );
}

#[test]
fn inspect_facet_claims_wire() {
    insta::assert_json_snapshot!(InspectFacetClaims {
        capability: capability(1),
        document_id: document(1),
        facet_key: key("main"),
        heads: vec![hash(1)],
    });
    insta::assert_json_snapshot!("facet_claims_inspection", claims_inspection());
}

#[test]
fn signature_evidence_wire() {
    for (name, evidence) in [
        ("signature_evidence_verified", verified_evidence()),
        (
            "signature_evidence_pending",
            SignatureEvidence::Pending {
                reason: "verification could not complete".into(),
            },
        ),
        (
            "signature_evidence_resource_refused",
            SignatureEvidence::ResourceRefused {
                reason: "budget exhausted".into(),
            },
        ),
        (
            "signature_evidence_unsupported",
            SignatureEvidence::Unsupported {
                reason: "claim v3".into(),
            },
        ),
        (
            "signature_evidence_proven_forgery",
            SignatureEvidence::ProvenForgery {
                reason: "bad signature".into(),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, evidence);
    }
}
