#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "projected_revision is a test-only generated query"
    )
)]
pub(crate) mod queries {
    include!("sqlite/gen.rs");
}
