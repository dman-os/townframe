use super::*;

mod user;

pub fn btress_api_features(reg: &TypeReg) -> Vec<Feature> {
    vec![user::feature(reg)]
}
