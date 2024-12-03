use crate::run_command;
use anyhow::Result;
use std::{fs::create_dir_all, path::Path};

pub fn generate_all_docs(current_dir: &Path) -> Result<()> {
    let root = current_dir.join("website").join("root");
    println!("Generating main");
    create_dir_all(root.join("docs")).unwrap();
    build_docs(
        current_dir,
        Path::new("docs"),
        &root.join("docs").join("main"),
    );

    Ok(())
}

fn build_docs(current_dir: &Path, in_path: &Path, out_path: &Path) {
    let temp_docs_dir = current_dir.join("target").join("temp_docs_build");
    std::fs::remove_dir_all(&temp_docs_dir).ok();
    run_command(
        in_path,
        "mdbook",
        &["build", "--dest-dir", temp_docs_dir.to_str().unwrap()],
    )
    .ok();

    std::fs::remove_dir_all(out_path).ok();
    std::fs::rename(temp_docs_dir.join("html"), out_path).unwrap();

    std::fs::remove_dir_all(&temp_docs_dir).ok();
}
