use super::*;

mod doc;

pub fn daybook_api_features(reg: &TypeReg) -> Vec<Feature> {
    vec![doc::feature(reg)]
}
