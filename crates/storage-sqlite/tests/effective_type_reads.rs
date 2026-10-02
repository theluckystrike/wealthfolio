//! An activity's type is its override when the override is not blank, else
//! its stored type, wherever it is read (engine rules §5). SQL reads it
//! through `effective_type_sql`, Rust through core's `type_override` and
//! `effective_activity_type`. This scans the workspace's code and fails on a
//! read that bypasses them, so a new query cannot reintroduce the blank
//! override as a type.

use std::fs;
use std::path::{Path, PathBuf};

const ROOTS: &[&str] = &["crates", "apps/server/src", "apps/tauri/src"];
/// Where the type is defined: core's helpers and the engine's normalize.
const DEFINITIONS: &[&str] = &[
    "crates/core/src/activities/activities_model.rs",
    "crates/portfolio-engine/src/normalize.rs",
];
/// Raw reads that treat a blank override as a type.
const RAW_RUST_READS: &[&str] = &[
    ".activity_type_override.is_none()",
    ".activity_type_override.is_some()",
    "activity_type_override.as_deref().unwrap_or(",
];

fn rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if path.is_dir() {
            if !matches!(
                name.as_str(),
                "target" | "tests" | "test_support" | "migrations" | "node_modules"
            ) {
                rust_files(&path, files);
            }
        } else if name.ends_with(".rs") && name != "tests.rs" && !name.ends_with("_tests.rs") {
            files.push(path);
        }
    }
}

/// Lowercased, without whitespace or line continuations, and without the
/// file's test module.
fn code(text: &str) -> String {
    let text = text.split("#[cfg(test)]").next().unwrap_or_default();
    text.chars()
        .filter(|c| !c.is_whitespace() && *c != '\\')
        .flat_map(char::to_lowercase)
        .collect()
}

/// `COALESCE(` or `IFNULL(` over the override, optionally table-qualified.
fn sql_reads(code: &str) -> usize {
    code.match_indices("activity_type_override")
        .filter(|(index, _)| {
            let mut before = &code[..*index];
            if let Some(qualified) = before.strip_suffix('.') {
                before = qualified.trim_end_matches(|c: char| c.is_alphanumeric() || c == '_');
            }
            before.ends_with("coalesce(") || before.ends_with("ifnull(")
        })
        .count()
}

#[test]
fn every_read_of_the_activity_type_treats_a_blank_override_as_none() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    for root in ROOTS {
        rust_files(&workspace.join(root), &mut files);
    }
    assert!(files.len() > 100, "the scan found the workspace's sources");

    let mut violations = Vec::new();
    for file in files {
        let relative = file
            .strip_prefix(&workspace)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        let code = code(&fs::read_to_string(&file).unwrap_or_default());
        if sql_reads(&code) > 0 {
            violations.push(format!("{relative}: SQL reads the override raw"));
        }
        if DEFINITIONS.contains(&relative.as_str()) {
            continue;
        }
        for read in RAW_RUST_READS {
            if code.contains(read) {
                violations.push(format!("{relative}: `{read}`"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "read the type through effective_type_sql or core's type_override:\n{}",
        violations.join("\n")
    );
}
