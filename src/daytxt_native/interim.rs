//! What the app installs on its node beyond the cartridges: intxt itself, and the grants of the
//! bundled micros that are in no cartridge yet (the review micro).
//!
//! INTERIM: the grants table below is the one place the app names a bundled micro's node access
//! until that micro is published as a cartridge, whose `uses` and grants then mint its handles
//! (see `clay_node`). A micro not listed still gets an actor and an egress, but can call no
//! method.

use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::DrawerAccess;
use clay_node::InstallConfig;
use clay_node::MicroInstall;
use clay_node::NodeComposition;
use service_utils_rs::RequestContext;
use std::path::Path;
use utils_rs::prelude::ferr;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;

#[cfg(test)]
mod e2e;

/// What one micro is granted by its plug name (the `<name>` of `<did>/<name>`), and the install
/// file that configures it.
///
/// INTERIM: see [`MicroInstall`] in `clay_node`. This table is the one place the composition names a micro's node
/// access. A micro not listed here still gets an actor and an egress, but can call no method.
struct InterimGrant {
    name: &'static str,
    /// The node methods the micro may call.
    methods: &'static [&'static str],
    /// The micro's install file, when it has a configuration datum an operator writes.
    config: Option<InterimConfig>,
}

/// The configuration datum of an [`InterimGrant`]: the file an operator writes for it (a JSON
/// object, which becomes the datum's value), where the datum goes, and which of the file's
/// members name drawers. Each member in `drawers` is a drawer id or an array of them; the micro is
/// granted `Read` over every drawer named. The drawers the members in `blob_drawers` name (a
/// subset of `drawers`) are the ones whose blobs the micro reads: its config gets a `blobCap`
/// reaching them (an empty reach when the file names none of them), and a spec with no
/// `blob_drawers` gets no `blobCap`.
struct InterimConfig {
    file: &'static str,
    tag: &'static str,
    key: &'static str,
    drawers: &'static [&'static str],
    blob_drawers: &'static [&'static str],
}

const INTERIM_GRANTS: &[InterimGrant] = &[InterimGrant {
    // intxt's review micro (D62): the linked intxts' summaries, and the PR and intxt datums it reads
    // with the doc capability in its config.
    name: "dayframe.review",
    methods: &[
        "org.example.intxt.summarizeIntxts",
        "org.example.daybook.docs.readDatums",
        "org.example.daybook.docs.pollDatums",
        // A commit's patch, which intxt keeps as a blob its intxt drawer's inventory pins.
        "org.example.daybook.blobs.read",
    ],
    config: Some(InterimConfig {
        file: "review.json",
        tag: micro_review_manifest::CONFIG_TAG,
        key: micro_review_manifest::CONFIG_KEY,
        drawers: &["githubDrawer", "intxtDrawers"],
        blob_drawers: &["intxtDrawers"],
    }),
}];

/// The installs of the micros with these ids, each granted its [`INTERIM_GRANTS`].
///
/// `install_file` answers the text of an install file by its name (`review.json`), `None` when
/// the operator has not written one. A micro whose file is absent installs unconfigured: no
/// drawers, no config datum, and so no document capability (the review micro shows
/// "Unconfigured"). A file that is not a JSON object, or names a drawer that is not a drawer
/// id, is an error: the operator wrote it, and a half-read file would grant the wrong drawers.
pub(crate) fn interim_installs(
    micros: impl IntoIterator<Item = String>,
    install_file: impl Fn(&str) -> std::io::Result<Option<Vec<u8>>>,
) -> Res<Vec<MicroInstall>> {
    micros
        .into_iter()
        .map(|micro| {
            let name = micro
                .split_once('/')
                .unwrap_or_else(|| panic!("micro id {micro:?} is not `<did>/<name>`"))
                .1;
            let granted = INTERIM_GRANTS.iter().find(|granted| granted.name == name);
            let (drawers, config) = match granted.and_then(|granted| granted.config.as_ref()) {
                Some(config) => match install_file(config.file)
                    .wrap_err_with(|| format!("reading {}", config.file))?
                {
                    Some(bytes) => {
                        let (drawers, config) = parse_install_file(config, &bytes)
                            .wrap_err_with(|| format!("reading {}", config.file))?;
                        (drawers, Some(config))
                    }
                    None => (Vec::new(), None),
                },
                None => (Vec::new(), None),
            };
            Ok(MicroInstall {
                methods: granted
                    .into_iter()
                    .flat_map(|granted| granted.methods)
                    .map(|nsid| (*nsid).to_owned())
                    .collect(),
                drawers,
                config,
                micro,
            })
        })
        .collect()
}

/// The bytes of the install file `name` from the first of `directories` that has it. Missing
/// everywhere is `None`, which installs the micro unconfigured (see [`interim_installs`]);
/// any other read failure is the operator's to see. The host decides where operators put install
/// files (the phone's pairing directories, the desktop's data directory).
pub(crate) fn read_install_file(
    directories: &[&Path],
    name: &str,
) -> std::io::Result<Option<Vec<u8>>> {
    for directory in directories {
        match std::fs::read(directory.join(name)) {
            Ok(bytes) => return Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

/// The drawers (read) and the configuration datum `bytes` states for `config`.
fn parse_install_file(
    config: &InterimConfig,
    bytes: &[u8],
) -> Res<(Vec<(DrawerId, DrawerAccess)>, InstallConfig)> {
    let serde_json::Value::Object(value) = serde_json::from_slice(bytes)? else {
        return Err(ferr!("the file is not a JSON object"));
    };
    let mut drawers = Vec::new();
    let mut blob_drawers = Vec::new();
    for member in config.drawers {
        let blobs = config.blob_drawers.contains(member);
        let listed: Vec<&str> = match value.get(*member) {
            None => continue,
            Some(serde_json::Value::String(id)) => vec![id.as_str()],
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .ok_or_else(|| ferr!("{member} holds something that is not a drawer id"))
                })
                .collect::<Res<_>>()?,
            Some(_) => return Err(ferr!("{member} is not a drawer id or an array of them")),
        };
        for id in listed {
            let drawer = id
                .parse::<DrawerId>()
                .map_err(|error| ferr!("{member}: {id:?} is not a drawer id: {error}"))?;
            if blobs && !blob_drawers.contains(&drawer) {
                blob_drawers.push(drawer.clone());
            }
            if !drawers.iter().any(|(held, _)| *held == drawer) {
                drawers.push((drawer, DrawerAccess::Read));
            }
        }
    }
    Ok((
        drawers,
        InstallConfig {
            tag: config.tag.to_owned(),
            key: config.key.to_owned(),
            value,
            // A micro whose spec names blob members always gets a `blobCap` (the review micro's
            // config requires one), reaching nothing when the file lists none of those drawers.
            blob_drawers: (!config.blob_drawers.is_empty()).then_some(blob_drawers),
        },
    ))
}

/// Installs intxt on `node` from its handler component `guest_wasm` and enables it, as the
/// node's operator, so the node admits intxt's datum Lexicons and serves its methods (the
/// review micro's `summarizeIntxts`). Idempotent by cartridge version, so a host calls it at
/// every boot.
pub(crate) async fn install_intxt(
    node: &NodeComposition,
    guest_wasm: &[u8],
) -> Result<intxt_core::install::Installed, intxt_core::methods::IntxtError> {
    let operator = intxt_core::client::Caller::new(
        node.operator_transport(),
        RequestContext::root(),
        None,
        "intxt-install".to_owned(),
    );
    // MERGE-QUESTION: intxt's install now takes the drawers its cartridge is granted (the
    // intxts the node holds at Write, ADR 049 §4.1); the `install_intxt` this host had was
    // written before that and grants none, as `day`'s own install call does. Whether the host
    // should grant the node's intxt drawers here is a design choice, not a merge one.
    intxt_core::install::install(&operator, guest_wasm, &[]).await
}
