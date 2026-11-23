use dioxus::prelude::*;

#[derive(PartialEq)]
struct Intxt {
    title: &'static str,
    description: &'static str,
    entries: &'static [Entry],
    comments: &'static [Comment],
}

#[derive(PartialEq)]
enum Entry {
    CommitRange {
        repo: &'static str,
        from: &'static str,
        to: &'static str,
    },
    Branch {
        repo: &'static str,
        name: &'static str,
    },
    Session {
        agent: &'static str,
        id: &'static str,
    },
}

#[derive(PartialEq)]
struct Comment {
    author: &'static str,
    body: &'static str,
}

static FIXTURE: Intxt = Intxt {
    title: "Make the intxt viewer stateless",
    description: "Review the viewer slice: everything it shows is derived from the drawer.",
    entries: &[
        Entry::CommitRange {
            repo: "townframe",
            from: "43f9b548",
            to: "aa0fcdcc",
        },
        Entry::Branch {
            repo: "townframe",
            name: "spike/intxt-web-ssr",
        },
        Entry::Session {
            agent: "claude",
            id: "013k65AAMJi9bejF5GWSm1De",
        },
    ],
    comments: &[
        Comment {
            author: "dman",
            body: "Does the session entry need a transcript range?",
        },
        Comment {
            author: "reviewer",
            body: "Branch entries should pin a head commit.",
        },
    ],
};

const STYLE: &str = "body{font:16px/1.5 system-ui,sans-serif;max-width:42rem;margin:2rem auto;padding:0 1rem}\
li{margin:.25rem 0}code{background:#eee;padding:0 .25rem;border-radius:3px}\
.comment{border-left:3px solid #999;padding-left:.75rem;margin:.75rem 0}";

#[component]
fn EntryRow(entry: &'static Entry) -> Element {
    let (kind, scope, what) = match entry {
        Entry::CommitRange { repo, from, to } => ("commits", *repo, format!("{from}..{to}")),
        Entry::Branch { repo, name } => ("branch", *repo, (*name).to_owned()),
        Entry::Session { agent, id } => ("session", *agent, (*id).to_owned()),
    };
    rsx! {
        li {
            strong { "{kind} " }
            "{scope} "
            code { "{what}" }
        }
    }
}

#[component]
fn IntxtView(drawer_id: String, intxt: &'static Intxt) -> Element {
    rsx! {
        main {
            header {
                h1 { "{intxt.title}" }
                p {
                    small {
                        "drawer "
                        code { "{drawer_id}" }
                    }
                }
                p { "{intxt.description}" }
            }
            section {
                h2 { "Entries" }
                ul {
                    for entry in intxt.entries {
                        EntryRow { entry }
                    }
                }
            }
            section {
                h2 { "Comments" }
                for comment in intxt.comments {
                    article { class: "comment",
                        p {
                            strong { "{comment.author}" }
                        }
                        p { "{comment.body}" }
                    }
                }
            }
        }
    }
}

#[component]
fn Shell(title: String, children: Element) -> Element {
    rsx! {
        head {
            meta { charset: "utf-8" }
            meta {
                name: "viewport",
                content: "width=device-width, initial-scale=1",
            }
            title { "{title}" }
            style { dangerous_inner_html: STYLE }
        }
        body { {children} }
    }
}

fn render(root: fn(String) -> Element, props: String) -> String {
    let mut dom = VirtualDom::new_with_props(root, props);
    dom.rebuild_in_place();
    let mut out = String::from("<!doctype html><html lang=\"en\">");
    out.push_str(&dioxus_ssr::render(&dom));
    out.push_str("</html>");
    out
}

fn drawer_page(drawer_id: String) -> Element {
    // The fixture stands in for a drawer lookup; only the id varies per route.
    rsx! {
        Shell { title: FIXTURE.title,
            IntxtView { drawer_id, intxt: &FIXTURE }
        }
    }
}

fn not_found_page(path: String) -> Element {
    rsx! {
        Shell { title: "not found",
            p { "no route for {path}" }
        }
    }
}

/// Hand-rolled routing: `/` renders the fixture, `/i/<drawer id>` renders it under that id.
pub fn route(path_with_query: &str) -> Option<String> {
    let path = path_with_query.split('?').next().unwrap_or("/");
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segments[..] {
        [] => Some(render(drawer_page, "fixture".to_owned())),
        ["i", id] => Some(render(drawer_page, id.to_owned())),
        _ => None,
    }
}

pub fn not_found(path: &str) -> String {
    render(not_found_page, path.to_owned())
}
