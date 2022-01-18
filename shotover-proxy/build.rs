use std::env;

fn main() {
    let profile = env::var("PROFILE").unwrap();
    println!("cargo:rustc-env=PROFILE={profile}");
}
