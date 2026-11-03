#[expect(dead_code, reason = "generated SQL queries are used by feature-specific callers")]
pub(crate) mod queries {
    include!("sqlite/gen.rs");
}
