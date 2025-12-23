use crate::interlude::*;

/// Versions work lik @foo/bar@1.2.3
pub struct PluginManifest {
    namespace: String,
    name: String,
    version: semver::Version,

    title: String,
    desc: String,
    // dependencies: Vec<String>,
    routines: HashMap<String, RoutineManifest>,
    // commands: Vec<PluginCommandManifest>,
    // processors: Vec<PluginProcessorManifest>,
}

pub struct RoutineManifest {}

pub enum RoutineManifestDeets {
    /// Routine that can be invoked on a document with rw access on whole doc
    DocInvoke {},
    /// Routine that is invoked when a pending prop is in a doc with ro access
    /// to doc but rw access on prop.
    DocProp {},
    // DocCollator {},
    // PredicateToDocProp {},
    // InvokeOnPredicate {},
}

struct CommandManifest {
    name: String,
    desc: String,

    // cli_unlisted: bool,
    // gui_unlisted: bool,
    deets: CommandDeets,
}

pub enum CommandDeets {
    // NOTE: behavior differs depending on the routine
    //  - if a DocInvoke, it's invoked
    //  - If a DocProp routine, the prop is added with pending payload
    //  - if InvokeOnPredicate, we re-check for predicates and run on matches
    //  - if PredicateToDocProp, we check predicates and add props
    //  - if DocCollator, we just run collator
    DocCommand { routine_name: String },
}

// struct PluginProcessorManifest {}

//
//
// enum CommandImpl {
//     Wflow(),
// }
