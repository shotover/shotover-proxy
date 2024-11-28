use crate::run_command;
use anyhow::Result;
use rinja::Template;
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

    // ensure repo exists and is up to date
    let repo_path = Path::new("website").join("shotover_repo_for_docs");
    if repo_path.exists() {
        run_command(&repo_path, "git", &["fetch"])?;
    } else {
        run_command(
            ".",
            "git",
            &[
                "clone",
                "https://github.com/shotover/shotover-proxy",
                repo_path.to_str().unwrap(),
            ],
        )?;
    }

    let versions = crate::version_tags::get_versions_of_repo(&repo_path);

    let docs = DocsTemplate {
        versions: versions.iter().map(|x| x.semver_range.clone()).collect(),
    };
    std::fs::write(root.join("docs").join("index.html"), docs.render().unwrap()).unwrap();

    for version in &versions {
        println!("Generating {}", version.tag);
        run_command(&repo_path, "git", &["checkout", &version.tag])?;

        build_docs(
            current_dir,
            &repo_path.join("docs"),
            &root.join("docs").join(&version.semver_range),
        )
    }

    if let Some(version) = versions.first() {
        println!("Generating latest");
        run_command(&repo_path, "git", &["checkout", &version.tag])?;

        build_docs(
            current_dir,
            &repo_path.join("docs"),
            &root.join("docs").join("latest"),
        )
    }

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

#[derive(Template)]
#[template(path = "docs.html")]
struct DocsTemplate {
    versions: Vec<String>,
}
