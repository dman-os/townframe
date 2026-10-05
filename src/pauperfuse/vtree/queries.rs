pub const FORMAT: &str = include_str!("../sql/queries/vtree/format.sql");
#[cfg(test)]
pub const EXPLAIN_FORMAT: &str = include_str!("../sql/queries/vtree/explain_format.sql");
pub const REGISTER: &str = include_str!("../sql/queries/vtree/register.sql");
#[cfg(test)]
pub const EXPLAIN_REGISTER: &str = include_str!("../sql/queries/vtree/explain_register.sql");
pub const LOOKUP: &str = include_str!("../sql/queries/vtree/lookup.sql");
#[cfg(test)]
pub const EXPLAIN_LOOKUP: &str = include_str!("../sql/queries/vtree/explain_lookup.sql");
pub const GENERATION: &str = include_str!("../sql/queries/vtree/generation.sql");
#[cfg(test)]
pub const EXPLAIN_GENERATION: &str = include_str!("../sql/queries/vtree/explain_generation.sql");
pub const CLEAR: &str = include_str!("../sql/queries/vtree/clear.sql");
#[cfg(test)]
pub const EXPLAIN_CLEAR: &str = include_str!("../sql/queries/vtree/explain_clear.sql");
pub const BUMP: &str = include_str!("../sql/queries/vtree/bump.sql");
#[cfg(test)]
pub const EXPLAIN_BUMP: &str = include_str!("../sql/queries/vtree/explain_bump.sql");
pub const INSERT: &str = include_str!("../sql/queries/vtree/insert.sql");
#[cfg(test)]
pub const EXPLAIN_INSERT: &str = include_str!("../sql/queries/vtree/explain_insert.sql");
pub const PAGE_START: &str = include_str!("../sql/queries/vtree/page_start.sql");
#[cfg(test)]
pub const EXPLAIN_PAGE_START: &str = include_str!("../sql/queries/vtree/explain_page_start.sql");
pub const PAGE_AFTER: &str = include_str!("../sql/queries/vtree/page_after.sql");
#[cfg(test)]
pub const EXPLAIN_PAGE_AFTER: &str = include_str!("../sql/queries/vtree/explain_page_after.sql");
pub const SET_FORMAT: &str = include_str!("../sql/queries/vtree/set_format.sql");
#[cfg(test)]
pub const EXPLAIN_SET_FORMAT: &str = include_str!("../sql/queries/vtree/explain_set_format.sql");

pub const CONVERSION_CREATE: &str = include_str!("../sql/queries/vtree/conversion_create.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_CREATE: &str =
    include_str!("../sql/queries/vtree/explain_conversion_create.sql");

pub const CONVERSION_START: &str = include_str!("../sql/queries/vtree/conversion_start.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_START: &str =
    include_str!("../sql/queries/vtree/explain_conversion_start.sql");

pub const CONVERSION_AFTER: &str = include_str!("../sql/queries/vtree/conversion_after.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_AFTER: &str =
    include_str!("../sql/queries/vtree/explain_conversion_after.sql");

pub const CONVERSION_INSERT: &str = include_str!("../sql/queries/vtree/conversion_insert.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_INSERT: &str =
    include_str!("../sql/queries/vtree/explain_conversion_insert.sql");

pub const CONVERSION_CLEAR: &str = include_str!("../sql/queries/vtree/conversion_clear.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_CLEAR: &str =
    include_str!("../sql/queries/vtree/explain_conversion_clear.sql");

pub const CONVERSION_INSTALL: &str = include_str!("../sql/queries/vtree/conversion_install.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_INSTALL: &str =
    include_str!("../sql/queries/vtree/explain_conversion_install.sql");

pub const CONVERSION_DROP: &str = include_str!("../sql/queries/vtree/conversion_drop.sql");
#[cfg(test)]
pub const EXPLAIN_CONVERSION_DROP: &str =
    include_str!("../sql/queries/vtree/explain_conversion_drop.sql");
