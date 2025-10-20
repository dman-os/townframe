use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // Parse command line arguments to detect if we're generating bindings
    let args: Vec<String> = env::args().collect();

    // Check if this is a generate command
    if args.len() >= 2 && args[1] == "generate" {
        // Find the --out-dir argument to locate the output directory
        let mut out_dir: Option<String> = None;
        for (i, arg) in args.iter().enumerate() {
            if arg == "--out-dir" && i + 1 < args.len() {
                out_dir = Some(args[i + 1].clone());
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
    // Look for the generated Kotlin file
    let real_out_dir = Path::new(out_dir)
        .join("org")
        .join("example")
        .join("daybook")
        .join("uniffi");
    // .join("daybook_ffi.kt");

    let mut kotlin_files = vec![];
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
    for path in kotlin_files {
        match fs::read_to_string(&path) {
            Ok(content) => {
                let modified_content = content
                    .split('\n')
                    .map(|line| {
                        if line.contains("@file") {
                            format!("{}\n@file:OptIn(kotlin.time.ExperimentalTime::class, kotlin.uuid.ExperimentalUuidApi::class)", line)
                        } else {
                            line.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

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
