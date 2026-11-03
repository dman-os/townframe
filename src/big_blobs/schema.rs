use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet_core::datum::Bundle;

pub fn registration() -> SchemaRegistrationGroup<'static> {
    let bundle = Bundle::new(&big_blobs_core::UNIVERSE, Vec::new());
    let registrations = bundle
        .lexicons()
        .expect("blob declarations publish lexicons")
        .into_iter()
        .map(|lexicon| SchemaRegistration::new(lexicon.id, lexicon.file));
    SchemaRegistrationGroup::new(Some(bundle), registrations, Vec::new())
}
