use anyhow::{anyhow, Result};
use clap::Parser;
use cli::Args;
use rinja::Template;
use std::{path::Path, process::Command};
use subprocess::{Exec, Redirection};

mod cli;
mod docs;
mod version_tags;

fn main() {
    // Set standard path to root of repo so this always runs in the same directory, regardless of where the user ran it from.
    let current_dir = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    std::env::set_current_dir(current_dir).unwrap();

    let args = Args::parse();

    println!("Ensuring mdbook is installed");
    // TODO: Once mdbook starts doing macos aarch64 binary releases we should download the release directly instead of compiling.
    //       https://github.com/rust-lang/mdBook/pull/2500
    if !Command::new("cargo")
        .args(["install", "mdbook", "--version", "0.4.43"])
        .status()
        .unwrap()
        .success()
    {
        return;
    }

    let root = current_dir.join("website").join("root");
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).unwrap();

    // copy assets
    let dest_assets = root.join("assets");
    std::fs::create_dir_all(&dest_assets).unwrap();
    for file in std::fs::read_dir("website/assets").unwrap() {
        let file = file.unwrap();
        std::fs::copy(file.path(), dest_assets.join(file.file_name())).unwrap();
    }
    // browsers expect to find the file here.
    std::fs::rename(
        "website/root/assets/favicon.ico",
        "website/root/favicon.ico",
    )
    .unwrap();

    // generate landing page
    std::fs::write(root.join("index.html"), Landing {}.render().unwrap()).unwrap();

    if let Err(err) = docs::generate_all_docs(current_dir) {
        println!("{err}");
        return;
    }

    if args.serve {
        println!("Hosting website at: http://localhost:8000");

        devserver_lib::run(
            "localhost",
            8000,
            current_dir.join("website").join("root").to_str().unwrap(),
            false,
            "",
        );
    } else {
        let out = current_dir.join("website").join("root");
        println!(
            "Succesfully generated website at: file://{}",
            out.to_str().unwrap()
        );
    }
}

pub fn run_command(dir: impl AsRef<Path>, command: &str, args: &[&str]) -> Result<String> {
    let data = Exec::cmd(command)
        .args(args)
        .cwd(dir)
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Merge)
        .capture()?;

    if data.exit_status.success() {
        Ok(data.stdout_str())
    } else {
        Err(anyhow!(
            "command {} {:?} exited with {:?} and output:\n{}",
            command,
            args,
            data.exit_status,
            data.stdout_str()
        ))
    }
}

#[derive(Template)]
#[template(path = "landing.html")]
struct Landing {}
