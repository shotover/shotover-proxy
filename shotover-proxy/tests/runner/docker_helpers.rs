use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;
pub struct RedisContainer {
    container_name: String,
    port: u16,
}
impl RedisContainer {
    pub async fn start(port: u16) -> Result<Self, String> {
        let container_name = format!("test-redis-{}", port);
        let _ = Command::new("docker")
            .args(&["rm", "-f", &container_name])
            .output();
        println!(
            " Starting Redis container '{}' on port {}",
            container_name, port
        );

        let output = Command::new("docker")
            .args(&[
                "run",
                "-d",
                "--name",
                &container_name,
                "-p",
                &format!("{}:6379", port),
                "redis:latest",
            ])
            .output()
            .map_err(|e| format!("Failed to start Redis container: {}", e))?;
        if !output.status.success() {
            return Err(format!(
                "Docker command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let container = Self {
            container_name,
            port,
        };

        container.wait_for_ready().await?;
        println!(
            "Redis container '{}' ready on port {}",
            container.container_name, port
        );
        Ok(container)
    }
    async fn wait_for_ready(&self) -> Result<(), String> {
        println!(" Waiting for Redis to be ready...");

        for attempt in 1..=10 {
            let output = Command::new("docker")
                .args(&["exec", &self.container_name, "redis-cli", "ping"])
                .output();

            match output {
                Ok(output) if output.status.success() => {
                    let response = String::from_utf8_lossy(&output.stdout);
                    if response.trim() == "PONG" {
                        println!(" Redis ready after {} attempts", attempt);
                        return Ok(());
                    }
                }

                _ => {
                    println!(" Attempt {}/10: Redis not ready yet", attempt);
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }

        Err("Redis failed to become ready after 10 attempts".to_string())
    }

    pub fn connection_url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }
    pub fn cleanup(&self) {
        println!(" Cleaning up Redis container '{}'", self.container_name);

        let _ = Command::new("docker")
            .args(&["stop", &self.container_name])
            .output();
        let _ = Command::new("docker")
            .args(&["rm", &self.container_name])
            .output();

        println!(" Redis container '{}' cleaned up", self.container_name);
    }
}

impl Drop for RedisContainer {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_redis_container_lifecycle() {
        let redis = RedisContainer::start(6379).await.unwrap();
        let output = Command::new("docker")
            .args(&["exec", &redis.container_name, "redis-cli", "ping"])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "PONG");
    }
}
