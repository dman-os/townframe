use crate::interlude::*;
mod create;

pub const TAG: api::Tag = api::Tag {
    name: "user",
    desc: "User mgmt.",
};

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: Uuid,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    pub updated_at: OffsetDateTime,
    #[schema(example = "alice@example.com")]
    pub email: Option<String>,
    #[schema(example = "hunter2")]
    pub username: String,
}

pub fn router() -> axum::Router<SharedContext> {
    axum::Router::new().merge(EndpointWrapper::new(create::CreateUser))
}

pub fn components(
    builder: utoipa::openapi::ComponentsBuilder,
) -> utoipa::openapi::ComponentsBuilder {
    let builder = create::CreateUser::components(builder);
    let mut schemas = vec![];
    <User as utoipa::ToSchema>::schemas(&mut schemas);
    builder.schemas_from_iter(schemas)
}

pub fn operations(
    builder: utoipa::openapi::PathsBuilder,
    prefix_path: &str,
) -> utoipa::openapi::PathsBuilder {
    [(
        create::CreateUser::PATH,
        <create::CreateUser as DocumentedEndpoint>::path_item(),
    )]
    .into_iter()
    .fold(builder, |builder, (path, item)| builder.path(path, item))
}

mod testing {
    use super::*;

    pub static USER_01: LazyLock<User> = LazyLock::new(|| User {
        id: uuid::uuid!("add83cdf-2ab3-443f-84dd-476d7984cf75"),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        username: "sabrina".into(),
        email: Some("hex.queen@teen.dj".into()),
    });
    // pub static USER_01: LazyLock<User> = LazyLock::new(|| User {
    //     id: uuid::uuid!("019567ed-91c6-70aa-810a-0216fef8553e"),
    //     created_at: OffsetDateTime::now_utc(),
    //     updated_at: OffsetDateTime::now_utc(),
    //     username: "reno".into(),
    //     email: Some("reno@dak.ota".into()),
    // });
}
