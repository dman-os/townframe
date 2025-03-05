use crate::interlude::*;
mod create;

pub const TAG: api::Tag = api::Tag {
    name: "user",
    desc: "User mgmt.",
};

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct User {
    pub username: String,
}

pub static USER_01: LazyLock<User> = LazyLock::new(|| User {
    username: "asdf".into(),
});
