use std::path::PathBuf;

pub fn windsock_path() -> PathBuf {
    // If we are run via cargo (we are in a target directory) use the target directory for storage.
    // Otherwise just fallback to the current working directory.
    let mut path = std::env::current_exe().unwrap();
    while path.pop() {
        if path.file_name().map(|x| x == "target").unwrap_or(false) {
            return path.join("windsock_data");
        }
    }

    PathBuf::from("windsock_data")
}
