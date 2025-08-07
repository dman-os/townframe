use crate::interlude::*;

use api_utils_rs::gen::*;

mod create;

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: Uuid,
    #[serde(with = "api_utils_rs::codecs::sane_iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "api_utils_rs::codecs::sane_iso8601")]
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

pub fn feature(reg: &TypeReg) -> Feature {
    let schema_user = reg.add_type(Type::Record(
        Record::builder()
            .name("User")
            .with_fields([
                ("id", RecordField::uuid(&reg).build()),
                ("created_at", RecordField::date_time(&reg).build()),
                ("updated_at", RecordField::date_time(&reg).build()),
                ("email", RecordField::email(&reg).optional(&reg).build()),
                ("username", RecordField::builder(reg.string()).build()),
            ])
            .build(),
    ));
    Feature {
        tag: api::Tag {
            name: "user",
            desc: "User mgmt.",
        },
        schema_types: vec![schema_user],
        endpoints: vec![create::epoint_type(reg, schema_user)],
    }
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
