mod interlude {
    pub use utils_rs::prelude::Arc;
    pub use utils_rs::prelude::Deserialize;
    pub use utils_rs::prelude::Res;
    pub use utils_rs::prelude::Serialize;
    pub use utils_rs::prelude::async_trait;
    pub use utils_rs::prelude::eyre;
    pub use utils_rs::prelude::ferr;
    pub use utils_rs::prelude::tokio;
}

pub mod r#gen;
pub mod kvstore;
pub mod partition;
