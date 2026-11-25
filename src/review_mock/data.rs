//! The mock review's data: three changes as the agent's commits produced them, each file a
//! unified patch, with the threads the agent and a reviewer left on them.

use std::borrow::Cow;

pub(crate) struct Change {
    pub(crate) title: &'static str,
    pub(crate) summary: &'static str,
    pub(crate) files: &'static [File],
}

pub(crate) struct File {
    pub(crate) path: &'static str,
    /// A unified patch of this one file, as the agent's commit produced it.
    pub(crate) patch: &'static str,
    pub(crate) threads: &'static [SeedThread],
}

/// A thread as the mock data seeds it: one new-file line, as the agent left it.
pub(crate) struct SeedThread {
    pub(crate) line: usize,
    pub(crate) resolved: bool,
    pub(crate) comments: &'static [Comment],
}

#[derive(Clone, PartialEq)]
pub(crate) struct Comment {
    pub(crate) author: &'static str,
    pub(crate) body: Cow<'static, str>,
}

pub(crate) const CHANGES: &[Change] = &[
    Change {
        title: "Cap the retry backoff",
        summary: "Retries doubled forever and overflowed after 64 attempts. The delay now saturates and is capped at thirty seconds, and a push wakes the queue's waiters.",
        files: &[
            File {
                path: "src/retry.rs",
                patch: "diff --git a/src/retry.rs b/src/retry.rs
--- a/src/retry.rs
+++ b/src/retry.rs
@@ -1,9 +1,12 @@
 use std::time::Duration;
 
-pub fn backoff(attempt: u32) -> Duration {
-    Duration::from_millis(100 * 2u64.pow(attempt))
+const CAP: Duration = Duration::from_secs(30);
+
+pub fn backoff(attempt: u32) -> Duration {
+    let delay = Duration::from_millis(100 * 2u64.saturating_pow(attempt));
+    delay.min(CAP)
 }
 
 pub fn should_retry(error: &Error) -> bool {
     error.is_transient()
 }
",
                threads: &[SeedThread {
                    line: 6,
                    resolved: false,
                    comments: &[
                        Comment {
                            author: "Reviewer",
                            body: Cow::Borrowed(
                                "The multiplication can still overflow before the cap applies; saturate that too.",
                            ),
                        },
                        Comment {
                            author: "Agent",
                            body: Cow::Borrowed(
                                "u64 saturating_pow covers the power; 100 * u64::MAX would not, so I will use saturating_mul.",
                            ),
                        },
                    ],
                }],
            },
            File {
                path: "src/queue.rs",
                patch: "diff --git a/src/queue.rs b/src/queue.rs
--- a/src/queue.rs
+++ b/src/queue.rs
@@ -10,6 +10,7 @@ impl Queue {
     pub fn push(&mut self, job: Job) {
         self.jobs.push_back(job);
+        self.notify.notify_one();
     }
 
     pub fn pop(&mut self) -> Option<Job> {
         self.jobs.pop_front()
",
                threads: &[],
            },
        ],
    },
    Change {
        title: "Validate config at startup",
        summary: "A bad config file panicked with an opaque unwrap. Loading now names what is wrong and rejects a zero worker count or port.",
        files: &[File {
            path: "src/config.rs",
            patch: "diff --git a/src/config.rs b/src/config.rs
--- a/src/config.rs
+++ b/src/config.rs
@@ -20,6 +20,9 @@
 impl Config {
     pub fn load(path: &Path) -> Config {
-        let text = fs::read_to_string(path).unwrap();
-        toml::from_str(&text).unwrap()
+        let text = fs::read_to_string(path).expect(\"config file is readable\");
+        let config: Config = toml::from_str(&text).expect(\"config is valid TOML\");
+        assert!(config.workers > 0, \"workers must be positive\");
+        assert!(config.port != 0, \"port must be set\");
+        config
     }
 }
",
            threads: &[SeedThread {
                line: 24,
                resolved: true,
                comments: &[Comment {
                    author: "Reviewer",
                    body: Cow::Borrowed(
                        "A user's config is input, not an invariant; this should be a returned error.",
                    ),
                }],
            }],
        }],
    },
    Change {
        title: "Document the sync retry",
        summary: "The module doc now says why retries are expected, so nobody parks them.",
        files: &[File {
            path: "src/sync.rs",
            patch: "diff --git a/src/sync.rs b/src/sync.rs
--- a/src/sync.rs
+++ b/src/sync.rs
@@ -1,4 +1,5 @@
-//! Sync.
+//! Pulls membership before documents.
+//! Retries are expected while membership lands.
 
 pub fn pull() {}
 pub fn push() {}
",
            threads: &[],
        }],
    },
];
