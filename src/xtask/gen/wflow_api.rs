use super::*;

pub mod metastore;
pub mod types;

pub fn wflow_api_features(reg: &TypeReg) -> Vec<Feature> {
    vec![metastore::feature(reg), types::feature(reg)]
}
