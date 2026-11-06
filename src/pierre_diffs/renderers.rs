// Slice S6: the diff renderers (`packages/diffs/src/renderers`).

pub mod diff_hunks_renderer;

#[cfg(test)]
mod annotations_test;
#[cfg(test)]
mod diff_hunks_renderer_test;
#[cfg(test)]
mod injected_row_hooks_test;
#[cfg(test)]
mod languages_test;
#[cfg(test)]
mod oracle_tests;
#[cfg(test)]
mod patch_file_render_test;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod virtualization_test;
