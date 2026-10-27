//! Snapshots pinning the serde wire form of every public DTO in [`super::protocol`], so a wire
//! change shows up as a reviewable snapshot diff. Fixtures are deterministic byte patterns.

use super::NOTE;
use super::PLACEMENT;
use super::datum_map;
use super::key;
use super::note;
use automerge::ChangeHash;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::dmeta::Bytes;
use big_cabinet_core::dmeta::ClaimOperation;
use big_cabinet_core::dmeta::DatumClaim;
use big_cabinet_core::dmeta::DatumSignature;
use big_cabinet_core::dmeta::MultibaseKey;
use big_cabinet_core::dmeta::SignatureBytes;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::dmeta::cid_of;
use big_cabinet_core::doc::*;
use serde_json::json;
use service_utils_rs::ContextError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

fn hash(byte: u8) -> ChangeHash {
    ChangeHash([byte; 32])
}

fn version(bytes: &[u8]) -> DatumVersion {
    DatumVersion {
        heads: bytes.iter().map(|byte| hash(*byte)).collect(),
        clock: 0,
    }
}

fn document(byte: u8) -> DocId {
    DocId::new([byte; 32])
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

fn claim() -> DatumClaim {
    DatumClaim {
        origin_document: document(1),
        datum_key: key("main"),
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
            signature: DatumSignature {
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

fn claims_inspection() -> DatumClaimsInspection {
    DatumClaimsInspection {
        doc_id: document(1),
        datum_key: key("main"),
        heads: vec![hash(7)],
        scope: EvidenceScope::UnsafeUnattributedSignatureEvidence,
        evidence: vec![
            DatumClaimEvidence {
                marker: "marker-1".into(),
                result: verified_evidence(),
            },
            DatumClaimEvidence {
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
            "write_mode_if_doc_unchanged",
            WriteMode::IfDocUnchanged {
                heads: vec![hash(1)],
            },
        ),
        (
            "write_mode_if_datums_unchanged",
            WriteMode::IfDatumsUnchanged {
                datums: BTreeMap::from([(key("main"), version(&[2]))]),
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
fn datum_read_wire() {
    for (name, read) in [
        (
            "datum_read_accepted",
            DatumRead::Accepted {
                value: note("hello"),
                version: version(&[1]),
            },
        ),
        (
            "datum_read_absent_written",
            DatumRead::Absent {
                version: version(&[2]),
            },
        ),
        (
            "datum_read_absent_never_written",
            DatumRead::Absent {
                version: version(&[]),
            },
        ),
        (
            "datum_read_pending",
            DatumRead::Pending {
                reason: "verification could not complete".into(),
                version: version(&[3]),
                value: None,
            },
        ),
        (
            "datum_read_pending_optimistic",
            DatumRead::Pending {
                reason: "verification could not complete".into(),
                version: version(&[3]),
                value: Some(note("unverified")),
            },
        ),
        (
            "datum_read_rejected",
            DatumRead::Rejected {
                reason: "unsigned value".into(),
            },
        ),
        (
            "datum_read_undecodable",
            DatumRead::Undecodable {
                reason: "not a string".into(),
                version: version(&[4]),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, read);
    }
}

#[test]
fn datums_read_wire() {
    insta::assert_json_snapshot!(DatumsRead {
        heads: vec![hash(1)],
        datums: BTreeMap::from([
            (
                key("main"),
                DatumRead::Accepted {
                    value: note("hello"),
                    version: version(&[2]),
                },
            ),
            (
                key("extra"),
                DatumRead::Absent {
                    version: version(&[]),
                }
            ),
        ]),
    });
}

#[test]
fn datum_listing_wire() {
    insta::assert_json_snapshot!(DatumListing {
        heads: vec![hash(1)],
        datums: BTreeMap::from([(key("main"), version(&[2, 3]))]),
    });
}

#[test]
fn datum_observed_wire() {
    for (name, observed) in [
        (
            "datum_observed_present",
            DatumObserved::Entry {
                revision: 7,
                doc_id: document(1),
                datum_key: key("main"),
                datum: ObservedDatum::Present {
                    doc_heads: vec![hash(1)],
                    version: version(&[2]),
                },
            },
        ),
        (
            "datum_observed_access_lost",
            DatumObserved::Entry {
                revision: 8,
                doc_id: document(1),
                datum_key: key("main"),
                datum: ObservedDatum::AccessLost,
            },
        ),
        (
            "datum_observed_replay_complete",
            DatumObserved::ReplayComplete { through: 9 },
        ),
    ] {
        insta::assert_json_snapshot!(name, observed);
    }
    insta::assert_json_snapshot!(
        "observe_datums_doc",
        ObserveDatums {
            capability: capability(1),
            scope: ObserveScope::Doc {
                doc_id: document(1)
            },
            after: 3,
        }
    );
}

#[test]
fn write_receipt_wire() {
    insta::assert_json_snapshot!(WriteReceipt {
        doc_id: document(1),
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
fn docs_error_wire() {
    for (name, error) in [
        ("error_context", DocsError::Context(ContextError::Cancelled)),
        ("error_denied", DocsError::Denied),
        (
            "error_authority",
            DocsError::Authority(AuthorityServiceError::InvalidCapability),
        ),
        (
            "error_authority_unavailable",
            DocsError::AuthorityUnavailable("connection refused".into()),
        ),
        ("error_access", DocsError::Access("placement denied".into())),
        (
            "error_repository",
            DocsError::Repository("disk full".into()),
        ),
        ("error_doc_not_found", DocsError::DocNotFound),
        (
            "error_unknown_heads",
            DocsError::UnknownHeads {
                heads: vec![hash(9)],
            },
        ),
        ("error_invalid_command", DocsError::InvalidCommand),
        (
            "error_command_in_flight",
            DocsError::CommandInFlight {
                command: command("cmd-1"),
            },
        ),
        (
            "error_command_reused",
            DocsError::CommandReused {
                command: command("cmd-1"),
            },
        ),
        (
            "error_unknown_schema",
            DocsError::UnknownSchema {
                datum_tag: NOTE.into(),
            },
        ),
        (
            "error_system_datum",
            DocsError::SystemDatum {
                datum_key: key("main"),
            },
        ),
        (
            "error_set_and_removed",
            DocsError::SetAndRemoved {
                datum_key: key("main"),
            },
        ),
        (
            "error_invalid_value",
            DocsError::InvalidValue {
                datum_key: key("main"),
                reason: "not a string".into(),
            },
        ),
        (
            "error_conflict",
            DocsError::Conflict {
                heads: vec![hash(3)],
                datums: BTreeMap::from([(key("main"), version(&[4]))]),
            },
        ),
        (
            "error_base_not_accepted",
            DocsError::BaseNotAccepted {
                datum_key: key("main"),
                verdict: Unaccepted::Rejected {
                    reason: "unsigned value".into(),
                },
            },
        ),
        (
            "error_pending",
            DocsError::Pending {
                reason: "inspection evidence pending".into(),
            },
        ),
        (
            "error_resource_refused",
            DocsError::ResourceRefused {
                reason: "budget exhausted".into(),
            },
        ),
        (
            "error_unsupported",
            DocsError::Unsupported {
                reason: "claim v3".into(),
            },
        ),
        (
            "error_malformed_evidence",
            DocsError::MalformedEvidence {
                reason: "truncated signature".into(),
            },
        ),
        ("error_unrelated_history", DocsError::UnrelatedHistory),
    ] {
        insta::assert_json_snapshot!(name, error);
    }
}

#[test]
fn create_doc_wire() {
    insta::assert_json_snapshot!(CreateDoc {
        capability: capability(1),
        command: command("create-note-1"),
        placement: PLACEMENT.into(),
        datums: datum_map(&[(&key("main"), note("hello"))]),
    });
}

#[test]
fn branch_doc_wire() {
    for (name, request) in [
        (
            "branch_doc_announced",
            BranchDoc {
                capability: capability(1),
                command: command("branch-1"),
                doc_id: document(1),
                at: At::Heads(vec![hash(1)]),
                path: "/draft".into(),
                stealth: false,
                placement: PLACEMENT.into(),
            },
        ),
        (
            "branch_doc_stealth",
            BranchDoc {
                capability: capability(1),
                command: command("branch-2"),
                doc_id: document(1),
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
fn merge_doc_wire() {
    for (name, request) in [
        (
            "merge_doc_with_command",
            MergeDoc {
                capability: capability(1),
                command: Some(command("merge-1")),
                source_id: document(2),
                at: At::Heads(vec![hash(2)]),
                target_id: document(1),
            },
        ),
        (
            "merge_doc_bare",
            MergeDoc {
                capability: capability(1),
                command: None,
                source_id: document(2),
                at: At::Current,
                target_id: document(1),
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, request);
    }
}

#[test]
fn write_datums_wire() {
    for (name, request) in [
        (
            "write_datums_with_command",
            WriteDatums {
                capability: capability(1),
                command: Some(command("write-1")),
                doc_id: document(1),
                mode: WriteMode::IfDocUnchanged {
                    heads: vec![hash(1)],
                },
                set: datum_map(&[(&key("main"), note("hello"))]),
                remove: BTreeSet::from([key("extra")]),
            },
        ),
        (
            "write_datums_bare",
            WriteDatums {
                capability: capability(1),
                command: None,
                doc_id: document(1),
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
fn read_datums_wire() {
    for (name, request) in [
        (
            "read_datums_all",
            ReadDatums {
                capability: capability(1),
                doc_id: document(1),
                at: At::Current,
                datums: None,
                optimistic: false,
            },
        ),
        (
            "read_datums_named_optimistic",
            ReadDatums {
                capability: capability(1),
                doc_id: document(1),
                at: At::Heads(vec![hash(1)]),
                datums: Some(BTreeSet::from([key("main")])),
                optimistic: true,
            },
        ),
    ] {
        insta::assert_json_snapshot!(name, request);
    }
}

#[test]
fn list_datums_wire() {
    insta::assert_json_snapshot!(ListDatums {
        capability: capability(1),
        doc_id: document(1),
        at: At::Heads(vec![hash(1)]),
    });
}

#[test]
fn unsafe_inspect_datum_wire() {
    insta::assert_json_snapshot!(UnsafeInspectDatum {
        capability: capability(1),
        doc_id: document(1),
        datum_key: key("main"),
        heads: vec![hash(1)],
    });
    insta::assert_json_snapshot!(
        "unsafe_datum_inspection",
        UnsafeDatumInspection {
            native_value: Some(Bytes::from(vec![4, 5, 6])),
            schema_known: true,
            claims: claims_inspection(),
        }
    );
}

#[test]
fn inspect_datum_claims_wire() {
    insta::assert_json_snapshot!(InspectDatumClaims {
        capability: capability(1),
        doc_id: document(1),
        datum_key: key("main"),
        heads: vec![hash(1)],
    });
    insta::assert_json_snapshot!("datum_claims_inspection", claims_inspection());
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
