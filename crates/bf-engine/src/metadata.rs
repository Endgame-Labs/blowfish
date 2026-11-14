//! Namespace settings use immutable, sequenced objects, independently of document WAL batches.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct Settings {
    incarnation: String,
    #[serde(default)]
    read_only: bool,
    pinning: Option<Value>,
}
impl Engine {
    pub(crate) async fn settings_for_copy(
        &self,
        request: &WriteRequest,
        source: &NamespaceState,
    ) -> Result<bool, ApiError> {
        let name = match request.copy_from_namespace.as_ref().expect("copy source") {
            CopyFrom::Name(name) => name,
            CopyFrom::Config {
                source_namespace, ..
            } => source_namespace,
        };
        Ok(self
            .settings(name, &source.created_at.to_rfc3339())
            .await?
            .1
            .read_only)
    }

    async fn settings(&self, ns: &str, incarnation: &str) -> Result<(u64, Settings), ApiError> {
        validate_namespace(ns)?;
        let objects = self
            .0
            .store
            .list(&format!("ns/{ns}/metadata/"))
            .await
            .map_err(storage_error)?;
        let latest = objects
            .iter()
            .filter_map(|o| {
                o.key
                    .rsplit('/')
                    .next()?
                    .strip_suffix(".json")?
                    .parse::<u64>()
                    .ok()
                    .map(|seq| (seq, &o.key))
            })
            .max_by_key(|(seq, _)| *seq);
        let Some((seq, key)) = latest else {
            return Ok((0, Settings::default()));
        };
        let settings: Settings =
            serde_json::from_slice(&self.0.store.get(key).await.map_err(storage_error)?)
                .map_err(|_| ApiError::new(500, "stored namespace settings are corrupt"))?;
        Ok((
            seq,
            if settings.incarnation == incarnation {
                settings
            } else {
                Settings::default()
            },
        ))
    }
    pub(crate) async fn check_writable(&self, ns: &str) -> Result<(), ApiError> {
        let state = match self.snapshot(ns, &Consistency::Strong).await {
            Ok((s, _)) => s,
            Err(e) if e.status == 404 => return Ok(()),
            Err(e) => return Err(e),
        };
        if self
            .settings(ns, &state.created_at.to_rfc3339())
            .await?
            .1
            .read_only
        {
            return Err(ApiError::bad(
                "💔 Writes not permitted. This namespace is read-only.",
            ));
        }
        Ok(())
    }
    pub(crate) async fn decorate_metadata(
        &self,
        ns: &str,
        incarnation: &str,
        metadata: &mut Value,
    ) -> Result<(), ApiError> {
        let (_, settings) = self.settings(ns, incarnation).await?;
        if settings.read_only {
            metadata["read_only"] = json!(true);
        }
        if let Some(pinning) = settings.pinning {
            metadata["pinning"] = pinning;
        }
        Ok(())
    }
    /// Persist read-only and pinning settings. Missing fields preserve their previous values.
    /// Settings are not a transactional barrier for document writes already in flight.
    pub async fn patch_metadata(&self, ns: &str, patch: Value) -> Result<Value, ApiError> {
        let patch = patch
            .as_object()
            .ok_or_else(|| ApiError::malformed("metadata patch must be an object"))?;
        let read_only = patch
            .get("read_only")
            .map(|v| {
                v.as_bool()
                    .ok_or_else(|| ApiError::malformed("read_only must be a boolean"))
            })
            .transpose()?;
        let pinning = patch
            .get("pinning")
            .map(|v| match v {
                Value::Null | Value::Bool(false) => Ok(None),
                Value::Bool(true) => Ok(Some(json!({"replicas":1}))),
                Value::Object(o) => {
                    let replicas = o
                        .get("replicas")
                        .map(|v| {
                            v.as_u64().filter(|n| *n >= 1).ok_or_else(|| {
                                ApiError::malformed("pinning.replicas must be a positive integer")
                            })
                        })
                        .transpose()?
                        .unwrap_or(1);
                    Ok(Some(json!({"replicas":replicas})))
                }
                _ => Err(ApiError::malformed(
                    "pinning must be a boolean, object, or null",
                )),
            })
            .transpose()?;
        let (state, _) = self.snapshot(ns, &Consistency::Strong).await?;
        let incarnation = state.created_at.to_rfc3339();
        loop {
            let (seq, mut settings) = self.settings(ns, &incarnation).await?;
            settings.incarnation = incarnation.clone();
            if let Some(value) = read_only {
                settings.read_only = value;
            }
            if let Some(value) = &pinning {
                settings.pinning = value.clone();
            }
            let key = format!("ns/{ns}/metadata/{:020}.json", seq + 1);
            match self
                .0
                .store
                .create(
                    &key,
                    serde_json::to_vec(&settings).expect("settings serialize"),
                )
                .await
                .map_err(storage_error)?
            {
                CreateResult::Conflict => continue,
                _ => break,
            }
        }
        self.metadata(ns).await
    }
}
