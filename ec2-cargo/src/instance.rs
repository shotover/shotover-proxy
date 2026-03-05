use aws_throwaway::Ec2Instance;
use cargo_metadata::Metadata;
use std::fs::Permissions;
use std::os::unix::prelude::PermissionsExt;

pub struct Instance(pub Ec2Instance);

impl Instance {
    pub async fn run_command(&self, command: &str) -> anyhow::Result<()> {
        let mut receiver = self.0.ssh().shell_stdout_lines(command).await;
        while let Some(line) = receiver.recv().await {
            match line {
                Ok(line) => println!("{line}"),
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    pub async fn rsync_push_shotover(&self, cargo_meta: &Metadata) {
        let project_root_dir = &cargo_meta.workspace_root;
        let address = self.0.public_ip().unwrap().to_string();

        self.rsync(
            cargo_meta,
            vec![
                "--exclude".to_owned(),
                "target".to_owned(),
                format!("{}/", project_root_dir),
                format!("ubuntu@{address}:/home/ubuntu/shotover"),
            ],
        )
        .await
    }

    pub async fn rsync_fetch_windsock_results(&self, cargo_meta: &Metadata) {
        let windsock_dir = &cargo_meta.target_directory.join("windsock_data");
        let address = self.0.public_ip().unwrap().to_string();

        self.rsync(
            cargo_meta,
            vec![
                format!("ubuntu@{address}:/home/ubuntu/shotover/target/windsock_data/"),
                windsock_dir.to_string(),
            ],
        )
        .await
    }

    async fn rsync(&self, cargo_meta: &Metadata, append_args: Vec<String>) {
        let target_dir = &cargo_meta.target_directory;

        let key_path = target_dir.join("ec2-cargo-privatekey");
        tokio::fs::remove_file(&key_path).await.ok();
        tokio::fs::write(&key_path, self.0.client_private_key())
            .await
            .unwrap();
        std::fs::set_permissions(&key_path, Permissions::from_mode(0o400)).unwrap();

        let known_hosts_path = target_dir.join("ec2-cargo-known_hosts");
        tokio::fs::write(&known_hosts_path, self.0.openssh_known_hosts_line())
            .await
            .unwrap();

        let mut args = vec![
            "--delete".to_owned(),
            "-e".to_owned(),
            format!(
                "ssh -i {} -o 'UserKnownHostsFile {}'",
                key_path, known_hosts_path
            ),
            "-ra".to_owned(),
        ];
        args.extend(append_args);

        let output = tokio::process::Command::new("rsync")
            .args(args)
            .output()
            .await
            .unwrap();
        if !output.status.success() {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            panic!("rsync failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
        }
    }
}
