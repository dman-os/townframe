use super::*;
use crate::Query;
use clay_collection::DemandToken;
use clay_host::Host;
use clay_host::HostedCollection;
use std::future::Future;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::task::Waker;

fn app(reader: CollectionReader<String>) -> Element {
    let view = use_collection(&reader);
    let query = Query::new().demand(DemandToken(serde_json::json!(null)));
    let rows = view.query(&query);
    let text = format!(
        "{:?}:{}:{}",
        view.query_coverage(&query),
        view.is_closed(),
        rows.len()
    );
    rsx! { "{text}" }
}

fn render(dom: &mut VirtualDom, tree: &mut clay_surface::SurfaceTree) {
    {
        let mut work = std::pin::pin!(dom.wait_for_work());
        assert_eq!(
            work.as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop())),
            Poll::Ready(())
        );
    }
    dom.render_immediate(tree);
}

#[test]
fn collection_hook_renders_coverage_without_rows_and_withdraws_on_revocation() {
    let host = Host::new();
    let provider = host.start("provider");
    let consumer = host.start("consumer");
    let collection = HostedCollection::<String>::new(&host, &provider);
    let mut dom = VirtualDom::new_with_props(app, collection.reader(&consumer));
    let mut tree = clay_surface::SurfaceTree::new();
    dom.rebuild(&mut tree);
    assert_eq!(tree.text_content(), "Partial:false:0");
    render(&mut dom, &mut tree);
    assert_eq!(tree.text_content(), "Complete:false:0");
    collection
        .writer(&provider)
        .unwrap()
        .insert("row".into())
        .unwrap();
    render(&mut dom, &mut tree);
    assert_eq!(tree.text_content(), "Complete:false:1");
    host.close_instance(&consumer).unwrap();
    render(&mut dom, &mut tree);
    assert_eq!(tree.text_content(), "Unknown:true:0");
    drop(dom);

    let consumer = host.start("unmounted");
    let mut dom = VirtualDom::new_with_props(app, collection.reader(&consumer));
    dom.rebuild(&mut tree);
    render(&mut dom, &mut tree);
    drop(dom);
    host.close();
}
