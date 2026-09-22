//! Span-census layer: one TSV line per closed span, keyed to the task
//! attribution conventions (`docs/scratch/task-attribution-conventions.md`).
//!
//! Purpose: counts and durations per task type, per worker, per parent — the
//! aggregate view the log stream cannot give (nextest discards passing tests'
//! output, so spans must land in a file to survive a run).
//!
//! Opt-in only: the layer is constructed solely when `TASK_CENSUS_FILE` is set
//! and non-empty. Unset, this module is entirely inert. Lines are
//!
//! ```text
//! uptime_millis \t span_name \t duration_millis \t parent_span_name \t worker \t task_id \t peer_id \t doc_id \t obj_id
//! ```
//!
//! with `-` for absent values, one `write_all` per line in append mode. Install
//! the layer **after** `EnvFilter` in the registry: the filter's callsite
//! interest applies to every layer in the chain, so the census only observes
//! spans the configured filter would have logged anyway.
use std::collections::HashMap;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::{Id, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

pub struct CensusLayer {
    start: Instant,
    file: Mutex<Option<std::fs::File>>,
    /// Span data between `on_new_span` and `on_close`. Closed-span data is
    /// removed and emitted on close; the registry lookup is not used at close
    /// time because the span data may already be gone.
    open: Mutex<HashMap<Id, OpenSpan>>,
}

struct OpenSpan {
    start: Instant,
    name: String,
    parent_name: Option<String>,
    fields: HashMap<&'static str, String>,
}

impl CensusLayer {
    /// `Some(layer)` when `TASK_CENSUS_FILE` is set and non-empty, else `None`
    /// (the registry is left untouched). A file that cannot be opened yields a
    /// layer that silently writes nothing — a census must never take the
    /// process under test down.
    pub fn from_env() -> Option<Self> {
        let path = std::env::var("TASK_CENSUS_FILE").ok()?;
        if path.is_empty() {
            return None;
        }
        let file = OpenOptions::new().append(true).create(true).open(path).ok();
        Some(Self {
            start: Instant::now(),
            file: Mutex::new(file),
            open: Mutex::new(HashMap::new()),
        })
    }

    fn uptime_millis(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    /// Appends one line. Every failure path here (poisoned mutex, closed file,
    /// write error) is swallowed on purpose: this is an observability side
    /// channel, and crashing or stalling the process under test to preserve
    /// census data would invert the layer's cost/benefit.
    fn write_line(&self, line: &str) {
        let mut guard = match self.file.lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Poisoned only if a panic escaped while writing; the next
                // line would be corrupt anyway, so dropping the census for
                // this process is the right degradation.
                return;
            }
        };
        if let Some(file) = guard.as_mut()
            && file.write_all(line.as_bytes()).is_err()
        {
            // Swallowed on purpose (see fn doc): the census never surfaces its
            // own IO failures to the process under test.
        }
    }

    /// The fields whose values are worth carrying out of the log stream, per
    /// the conventions doc's vocabulary. Everything else stays in the log.
    const NAMED_FIELDS: [&'static str; 5] = ["worker", "task_id", "peer_id", "doc_id", "obj_id"];

    fn named_fields(fields: &HashMap<&'static str, String>) -> String {
        let mut line = String::new();
        for name in Self::NAMED_FIELDS {
            let value = fields.get(name).map(String::as_str).unwrap_or("-");
            if !line.is_empty() {
                line.push('\t');
            }
            line.push_str(if value.is_empty() { "-" } else { value });
        }
        line
    }
}

struct FieldCollector<'a> {
    fields: &'a mut HashMap<&'static str, String>,
}

impl Visit for FieldCollector<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // Display fields (`%value`) reach the visitor as Debug values whose
        // Debug renders the Display text, so one formatting path covers both.
        self.fields.insert(field.name(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for CensusLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut FieldCollector {
            fields: &mut fields,
        });
        let parent_name = attrs
            .parent()
            .and_then(|parent| ctx.span(parent))
            .map(|parent| parent.metadata().name().to_string());
        let entry = OpenSpan {
            start: Instant::now(),
            name: attrs.metadata().name().to_string(),
            parent_name,
            fields,
        };
        // A poisoned map only loses later spans' lines for this process; the
        // census must never take the process down, so the entry is dropped.
        if let Ok(mut open) = self.open.lock() {
            open.insert(id.clone(), entry);
        }
    }

    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
        let mut open = match self.open.lock() {
            Ok(open) => open,
            Err(_) => return,
        };
        let Some(entry) = open.get_mut(id) else {
            return;
        };
        values.record(&mut FieldCollector {
            fields: &mut entry.fields,
        });
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        let entry = self.open.lock().ok().and_then(|mut open| open.remove(&id));
        let Some(entry) = entry else {
            return;
        };
        let duration = entry.start.elapsed().as_millis();
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\n",
            self.uptime_millis(),
            entry.name,
            duration,
            entry.parent_name.as_deref().unwrap_or("-"),
            Self::named_fields(&entry.fields),
        );
        self.write_line(&line);
    }
}
