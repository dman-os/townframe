//! The small universe the laws draw from, and its writes by index, for the laws and the
//! crate's other tests over a part store.

use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::ObjPayload;
use crate::part_store::PartStoreWrite;
use crate::part_store::oracle::PartWrite;

/// The small universe every law and generated sequence draws from.
pub fn obj(index: u8) -> ObjKey {
    ObjKey::new(&[index])
}
pub fn part(index: u8) -> PartKey {
    PartKey::new(&[100 + index])
}
pub fn principal(index: u8) -> PeerKey {
    PeerKey::new(&[200 + index])
}
pub fn payload(value: u8) -> ObjPayload {
    serde_json::json!(value)
}
pub(super) const OBJS: u8 = 4;
pub(super) const PARTS: u8 = 3;
pub(super) const PRINCIPALS: u8 = 3;

/// Stage one batch through the trait.
pub async fn stage<S: PartStoreWrite>(store: &S, tx: &mut S::Tx, write: PartWrite) -> Res<()> {
    match write {
        PartWrite::EnsureParts(parts) => store.ensure_parts(tx, &parts).await,
        PartWrite::SetPayloads(payloads) => store.set_payloads(tx, payloads).await,
        PartWrite::AddMembers(members) => store.add_members(tx, &members).await,
        PartWrite::RemoveMembers(members) => store.remove_members(tx, &members).await,
        PartWrite::DropPayloads(objs) => store.drop_payloads(tx, &objs).await,
        PartWrite::GrantAccess(grants) => store.grant_access(tx, &grants).await,
        PartWrite::RevokeAccess(revokes) => store.revoke_access(tx, &revokes).await,
    }
}

/// One batch over the universe by index: `(obj, part)` for members, `(obj, value)` for payloads,
/// `(part, principal)` for access (granted at [`AccessLevel::Read`]).
pub fn add(members: &[(u8, u8)]) -> PartWrite {
    PartWrite::AddMembers(members.iter().map(|&(o, p)| (obj(o), part(p))).collect())
}
pub fn remove(members: &[(u8, u8)]) -> PartWrite {
    PartWrite::RemoveMembers(members.iter().map(|&(o, p)| (obj(o), part(p))).collect())
}
pub fn set(payloads: &[(u8, u8)]) -> PartWrite {
    PartWrite::SetPayloads(
        payloads
            .iter()
            .map(|&(o, value)| (obj(o), payload(value)))
            .collect(),
    )
}
pub fn drop_payloads(objs: &[u8]) -> PartWrite {
    PartWrite::DropPayloads(objs.iter().map(|&o| obj(o)).collect())
}
pub fn grant(grants: &[(u8, u8)]) -> PartWrite {
    PartWrite::GrantAccess(
        grants
            .iter()
            .map(|&(p, u)| (part(p), principal(u), AccessLevel::Read))
            .collect(),
    )
}
pub fn revoke(revokes: &[(u8, u8)]) -> PartWrite {
    PartWrite::RevokeAccess(
        revokes
            .iter()
            .map(|&(p, u)| (part(p), principal(u)))
            .collect(),
    )
}
