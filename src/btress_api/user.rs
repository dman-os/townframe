use crate::interlude::*;

use crate::gen::user::*;

pub mod create;

pub static USERNAME_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z0-9]+([_-]?[a-zA-Z0-9])*$").unwrap());

mod testing {
    use super::*;

    pub static USER_01: LazyLock<User> = LazyLock::new(|| User {
        // id: uuid::uuid!("add83cdf-2ab3-443f-84dd-476d7984cf75"),
        id: "add83cdf-2ab3-443f-84dd-476d7984cf75".into(),
        created_at: Timestamp::now().into(),
        updated_at: Timestamp::now().into(),
        username: "sabrina".into(),
        email: Some("hex.queen@teen.dj".into()),
    });
    // pub static USER_01: LazyLock<User> = LazyLock::new(|| User {
    //     id: uuid::uuid!("019567ed-91c6-70aa-810a-0216fef8553e"),
    //     created_at: Timestamp::now(),
    //     updated_at: Timestamp::now(),
    //     username: "reno".into(),
    //     email: Some("reno@dak.ota".into()),
    // });
}
