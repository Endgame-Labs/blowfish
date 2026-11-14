//! HTTP client for API key administration.
use bf_auth::{CreateKeyRequest, CreateKeyResponse, KeyInfo, ListKeysResponse, Role};
use clap::{Args, Subcommand};
use serde::Serialize;

#[derive(Args)]
pub(crate) struct Keys {
    /// Server base URL.
    #[arg(
        long,
        global = true,
        env = "BLOWFISH_URL",
        default_value = "http://127.0.0.1:8000"
    )]
    url: String,
    /// Super key, or a key with the keys role.
    #[arg(
        long,
        global = true,
        env = "BLOWFISH_ADMIN_KEY",
        hide_env_values = true
    )]
    admin_key: Option<String>,
    /// Emit JSON instead of a table.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: KeyCommand,
}
#[derive(Subcommand)]
enum KeyCommand {
    /// Create a key, displaying its token once.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long = "role", required = true, value_parser = parse_role)]
        roles: Vec<Role>,
        #[arg(long = "prefix")]
        prefixes: Vec<String>,
        /// Time until expiry, such as 30d or 12h.
        #[arg(long)]
        expires: Option<humantime::Duration>,
    },
    /// List active keys without their tokens or hashes.
    List,
    /// Revoke an API key by ID.
    Revoke { id: String },
}
fn parse_role(value: &str) -> Result<Role, String> {
    serde_json::from_value(serde_json::Value::String(value.into()))
        .map_err(|_| "expected admin, write, read, list, metrics, or keys".into())
}
impl Keys {
    pub(crate) async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let admin_key = self
            .admin_key
            .filter(|s| !s.is_empty())
            .ok_or("provide --admin-key or BLOWFISH_ADMIN_KEY")?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let base = format!("{}/_blowfish/v1/keys", self.url.trim_end_matches('/'));
        match self.command {
            KeyCommand::Create {
                name,
                roles,
                prefixes,
                expires,
            } => {
                let expires_at = expires
                    .map(|duration| {
                        chrono::Duration::from_std(*duration)
                            .ok()
                            .and_then(|duration| chrono::Utc::now().checked_add_signed(duration))
                            .ok_or("expiry is out of range")
                    })
                    .transpose()?;
                let spec = CreateKeyRequest {
                    name,
                    roles,
                    namespace_prefixes: if prefixes.is_empty() {
                        None
                    } else {
                        Some(prefixes)
                    },
                    expires_at,
                };
                let response = checked(
                    client
                        .post(&base)
                        .bearer_auth(&admin_key)
                        .json(&spec)
                        .send()
                        .await?,
                )
                .await?;
                let created: CreateKeyResponse = response.json().await?;
                if self.json {
                    print_json(&created)?;
                } else {
                    print_keys(&[created.key]);
                    println!("\nToken (save it now; shown only once):\n{}", created.token);
                }
            }
            KeyCommand::List => {
                let response =
                    checked(client.get(&base).bearer_auth(&admin_key).send().await?).await?;
                let keys: ListKeysResponse = response.json().await?;
                if self.json {
                    print_json(&keys)?;
                } else {
                    print_keys(&keys.keys);
                }
            }
            KeyCommand::Revoke { id } => {
                // IDs are generated hex, and must never be interpreted as URL paths.
                if id.len() != 32 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
                    return Err("invalid key ID".into());
                }
                let response = checked(
                    client
                        .delete(format!("{base}/{id}"))
                        .bearer_auth(&admin_key)
                        .send()
                        .await?,
                )
                .await?;
                if self.json {
                    print_json(&response.json::<serde_json::Value>().await?)?;
                } else {
                    println!("Revoked {id}");
                }
            }
        }
        Ok(())
    }
}
async fn checked(
    response: reqwest::Response,
) -> Result<reqwest::Response, Box<dyn std::error::Error>> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body: serde_json::Value = response.json().await?;
    Err(format!(
        "server returned {status}: {}",
        body["error"].as_str().unwrap_or("request failed")
    )
    .into())
}
fn print_json(value: &impl Serialize) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
fn print_keys(keys: &[KeyInfo]) {
    println!(
        "{:<32}  {:<20}  {:<24}  {:<20}  EXPIRES",
        "ID", "NAME", "ROLES", "PREFIXES"
    );
    for key in keys {
        let roles = key
            .roles
            .iter()
            .map(|r| {
                serde_json::to_value(r)
                    .expect("role serializes")
                    .as_str()
                    .expect("role is string")
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<32}  {:<20}  {:<24}  {:<20}  {}",
            key.id,
            key.name.escape_default(),
            roles,
            key.namespace_prefixes
                .as_ref()
                .map(|p| p.join(",").escape_default().to_string())
                .unwrap_or_else(|| "*".into()),
            key.expires_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "never".into())
        );
    }
}
