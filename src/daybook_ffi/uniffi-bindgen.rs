use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use regex::Regex;

fn main() {
    // Parse command line arguments to detect if we're generating bindings
    let args: Vec<String> = env::args().collect();

    // Check if this is a generate command
    if args.len() >= 2 && args[1] == "generate" {
        // Find the --out-dir argument to locate the output directory
        let mut out_dir: Option<String> = None;
        for (idx, arg) in args.iter().enumerate() {
            if arg == "--out-dir" && idx + 1 < args.len() {
                out_dir = Some(args[idx + 1].clone());
                break;
            }
        }

        // Run the original uniffi bindgen
        uniffi::uniffi_bindgen_main();

        // Apply post-processing modifications if we have an output directory
        if let Some(out_dir) = out_dir {
            apply_kotlin_modifications(&out_dir);
        }
    } else {
        // For non-generate commands, just run the original
        uniffi::uniffi_bindgen_main();
    }
}

fn apply_kotlin_modifications(out_dir: &str) {
    let real_out_dir = Path::new(out_dir)
        .join("org")
        .join("example")
        .join("daybook")
        .join("uniffi");

    let mut kotlin_files: Vec<PathBuf> = vec![];
    let mut dirs = vec![real_out_dir];
    loop {
        let Some(dir_path) = dirs.pop() else {
            break;
        };
        for file in std::fs::read_dir(dir_path).expect("dir not found") {
            let file = file.expect("error iterating dir");
            let path = file.path();
            let file_ty = file.file_type().expect("error reading file type");
            if file_ty.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "kt" {
                        kotlin_files.push(path)
                    }
                }
            } else if file_ty.is_dir() {
                dirs.push(path)
            }
        }
    }

    let file_opt_in =
        "@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)";
    let file_opt_in_re = Regex::new(r"(?m)^@file:.*$").expect("invalid regex");
    let data_class_re =
        Regex::new(r"(?m)^(@Serializable\s*\n)?(data class\s+([A-Za-z0-9_]+)\s*\()")
            .expect("invalid regex");
    let import_re = Regex::new(r"(?m)^import .+\n").expect("invalid regex");
    let package_re = Regex::new(r"(?m)^package .+\n").expect("invalid regex");
    let serializable_types: HashSet<&str> = ["Blob", "Note", "ImageMetadata", "Body"]
        .into_iter()
        .collect();

    for path in kotlin_files {
        match fs::read_to_string(&path) {
            Ok(content) => {
                let mut modified_content = content;
                if !modified_content.contains(file_opt_in) {
                    modified_content = file_opt_in_re
                        .replace(&modified_content, |captures: &regex::Captures| {
                            format!("{}\n{}", &captures[0], file_opt_in)
                        })
                        .to_string();
                }

                let is_daybook_types_file = path.ends_with("uniffi/types/daybook_types.kt");
                if is_daybook_types_file {
                    if !modified_content.contains("import kotlinx.serialization.Serializable") {
                        let insertion_index = import_re
                            .find_iter(&modified_content)
                            .last()
                            .map(|mat| mat.end())
                            .or_else(|| package_re.find(&modified_content).map(|mat| mat.end()));
                        if let Some(index) = insertion_index {
                            modified_content
                                .insert_str(index, "import kotlinx.serialization.Serializable\n");
                        }
                    }
                    modified_content = data_class_re
                        .replace_all(&modified_content, |captures: &regex::Captures| {
                            let type_name =
                                captures.get(3).map(|group| group.as_str()).unwrap_or("");
                            if !serializable_types.contains(type_name) || captures.get(1).is_some()
                            {
                                captures[0].to_string()
                            } else {
                                format!("@Serializable\n{}", &captures[2])
                            }
                        })
                        .to_string();
                }

                if let Err(err) = fs::write(&path, modified_content) {
                    panic!("Warning: Failed to apply Kotlin modifications: {err}");
                }
            }
            Err(err) => {
                panic!("Warning: Failed to read generated Kotlin file: {err}");
            }
        }
    }
}
