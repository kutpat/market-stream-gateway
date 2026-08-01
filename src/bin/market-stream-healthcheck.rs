use std::time::Duration;

#[tokio::main]
async fn main() {
    let url = std::env::var("MSG_HEALTHCHECK_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080/health/ready".to_owned());
    let result = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build healthcheck client")
        .get(url)
        .send()
        .await;
    match result {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => {
            eprintln!("gateway readiness returned {}", response.status());
            std::process::exit(1);
        }
        Err(error) => {
            eprintln!("gateway readiness request failed: {error}");
            std::process::exit(1);
        }
    }
}
