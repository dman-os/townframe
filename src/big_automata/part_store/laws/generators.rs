//! Generators for the part store's differential: writes, routes, selectors and ops.

use super::*;

pub(super) fn any_obj() -> impl Strategy<Value = ObjKey> {
    (0..OBJS).prop_map(obj)
}
pub(super) fn any_part() -> impl Strategy<Value = PartKey> {
    (0..PARTS).prop_map(part)
}
pub(super) fn any_principal() -> impl Strategy<Value = PeerKey> {
    (0..PRINCIPALS).prop_map(principal)
}
pub(super) fn any_payload() -> impl Strategy<Value = ObjPayload> {
    (0u8..3).prop_map(payload)
}
pub(super) fn any_level() -> impl Strategy<Value = AccessLevel> {
    prop_oneof![
        Just(AccessLevel::Pull),
        Just(AccessLevel::Read),
        Just(AccessLevel::Write),
        Just(AccessLevel::Admin),
    ]
}

pub fn part_writes() -> impl Strategy<Value = Vec<PartWrite>> {
    use proptest::collection::vec;
    let write = prop_oneof![
        1 => vec(any_part(), 0..2).prop_map(PartWrite::EnsureParts),
        3 => vec((any_obj(), any_payload()), 1..3).prop_map(PartWrite::SetPayloads),
        3 => vec((any_obj(), any_part()), 1..3).prop_map(PartWrite::AddMembers),
        2 => vec((any_obj(), any_part()), 1..3).prop_map(PartWrite::RemoveMembers),
        2 => vec(any_obj(), 1..3).prop_map(PartWrite::DropPayloads),
        2 => vec((any_part(), any_principal(), any_level()), 1..3).prop_map(PartWrite::GrantAccess),
        1 => vec((any_part(), any_principal()), 1..3).prop_map(PartWrite::RevokeAccess),
    ];
    vec(write, 1..4)
}

pub(super) fn any_routes() -> impl Strategy<Value = LogRoutes> {
    use proptest::collection::btree_map;
    (
        btree_map(any_part(), 0u64..6, 0..3),
        btree_map(any_obj(), 0u64..6, 0..3),
    )
        .prop_map(|(parts, objects)| LogRoutes { parts, objects })
}

pub(super) fn any_selector() -> impl Strategy<Value = LogSelector> {
    prop_oneof![
        Just(LogSelector::All),
        any_routes().prop_map(LogSelector::Routes),
    ]
}

pub fn part_ops() -> impl Strategy<Value = Vec<PartOp>> {
    use proptest::collection::vec;
    let op = prop_oneof![
        4 => part_writes().prop_map(PartOp::Commit),
        1 => part_writes().prop_map(PartOp::Rollback),
        1 => Just(PartOp::MintGeneration),
        2 => (any_selector(), any::<u8>(), any::<u8>(), any_limits()).prop_map(
            |(selector, after, until, limits)| PartOp::Read { selector, after, until, limits }
        ),
        2 => (any_principal(), any_routes(), any::<u8>(), any::<u8>(), any_limits()).prop_map(
            |(principal, routes, after, until, limits)| PartOp::ReadAuthorized {
                principal, routes, after, until, limits
            }
        ),
    ];
    vec(op, 0..25)
}
