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
    let kotlin_file_path = Path::new(out_dir)
        .join("org")
        .join("example")
        .join("daybook")
        .join("uniffi")
        .join("daybook_core.kt");
    
    if kotlin_file_path.exists() {
        match fs::read_to_string(&kotlin_file_path) {
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
                
                if let Err(e) = fs::write(&kotlin_file_path, modified_content) {
                    eprintln!("Warning: Failed to apply Kotlin modifications: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Warning: Failed to read generated Kotlin file: {}", e);
            }
        }
    }
}
