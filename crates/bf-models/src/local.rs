//! In-process GGUF inference through llama.cpp (cargo feature `local-models`).
//!
//! Without the feature, `provider = "local"` is rejected at startup with a clear message.
//! With it, the GGUF file is downloaded from Hugging Face on first use (or read from `path`),
//! loaded once, and run on a dedicated bounded thread pool, so embedding and reranking load
//! cannot starve query or indexing threads. A full queue answers 429.
use crate::{ModelConfig, ModelOptions};
use bf_types::ApiError;

/// Vectors (one per input) and the tokens processed.
pub(crate) struct Output {
    pub vectors: Vec<Vec<f32>>,
    pub tokens: u64,
}

#[cfg(not(feature = "local-models"))]
pub(crate) struct Local;
#[cfg(not(feature = "local-models"))]
impl Local {
    pub fn new(name: &str, _: &ModelConfig, _: ModelOptions) -> Result<Self, String> {
        Err(format!(
            "model '{name}': provider \"local\" needs a build with the `local-models` cargo feature"
        ))
    }
    pub async fn embed(&self, _: &[String]) -> Result<Output, ApiError> {
        unreachable!("local models are not compiled in")
    }
    pub async fn rerank(&self, _: &str, _: &[String]) -> Result<Output, ApiError> {
        unreachable!("local models are not compiled in")
    }
}

#[cfg(feature = "local-models")]
pub(crate) use real::Local;

#[cfg(feature = "local-models")]
mod real {
    use super::*;
    use crate::Task;
    use llama_cpp_4::{
        context::{LlamaContext, params::LlamaContextParams, params::LlamaPoolingType},
        llama_backend::LlamaBackend,
        llama_batch::LlamaBatch,
        model::{AddBos, LlamaModel, Special, params::LlamaModelParams},
        token::LlamaToken,
    };
    use sha2::{Digest, Sha256};
    use std::{
        num::NonZeroU32,
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex, OnceLock,
            mpsc::{Receiver, SyncSender, TrySendError},
        },
    };
    use tokio::sync::{OnceCell, oneshot};

    enum Job {
        Embed {
            texts: Vec<String>,
            reply: oneshot::Sender<Result<Output, String>>,
        },
        Rerank {
            query: String,
            docs: Vec<String>,
            reply: oneshot::Sender<Result<Output, String>>,
        },
    }
    struct Pool {
        tx: SyncSender<Job>,
    }
    pub(crate) struct Local {
        name: String,
        task: Task,
        cfg: ModelConfig,
        options: ModelOptions,
        pool: OnceCell<Arc<Pool>>,
    }

    fn backend() -> Result<&'static LlamaBackend, String> {
        static BACKEND: OnceLock<Result<LlamaBackend, String>> = OnceLock::new();
        BACKEND
            .get_or_init(|| {
                let mut b = LlamaBackend::init().map_err(|e| e.to_string())?;
                if std::env::var_os("BLOWFISH_LLAMA_LOGS").is_none() {
                    b.void_logs();
                }
                Ok(b)
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    impl Local {
        pub fn new(name: &str, c: &ModelConfig, options: ModelOptions) -> Result<Self, String> {
            let task = c.task.expect("checked by the registry");
            if c.path.is_none() && (c.hf_repo.is_none() || c.hf_file.is_none()) {
                return Err(format!(
                    "model '{name}': local models need hf_repo and hf_file, or path"
                ));
            }
            Ok(Self {
                name: name.into(),
                task,
                cfg: c.clone(),
                options,
                pool: OnceCell::new(),
            })
        }

        async fn pool(&self) -> Result<&Arc<Pool>, ApiError> {
            self.pool
                .get_or_try_init(|| async {
                    let path = self.ensure_file().await.map_err(|e| self.fail(&e))?;
                    let cfg = self.cfg.clone();
                    let task = self.task;
                    let name = self.name.clone();
                    tokio::task::spawn_blocking(move || start(&name, task, &cfg, &path))
                        .await
                        .map_err(|e| self.fail(&e.to_string()))?
                        .map(Arc::new)
                        .map_err(|e| self.fail(&e))
                })
                .await
        }
        fn fail(&self, what: &str) -> ApiError {
            tracing::error!(model = %self.name, error = %what, "local model failed");
            ApiError::new(
                503,
                format!("local model '{}' is unavailable: {what}", self.name),
            )
        }

        async fn submit(
            &self,
            make: impl FnOnce(oneshot::Sender<Result<Output, String>>) -> Job,
        ) -> Result<Output, ApiError> {
            let pool = self.pool().await?;
            let (reply, rx) = oneshot::channel();
            match pool.tx.try_send(make(reply)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    return Err(ApiError::new(
                        429,
                        format!("local model '{}' is busy; its queue is full", self.name),
                    ));
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(self.fail("inference threads stopped"));
                }
            }
            rx.await
                .map_err(|_| self.fail("inference thread stopped"))?
                .map_err(|e| ApiError::new(502, format!("local model '{}': {e}", self.name)))
        }
        pub async fn embed(&self, texts: &[String]) -> Result<Output, ApiError> {
            self.submit(|reply| Job::Embed {
                texts: texts.to_vec(),
                reply,
            })
            .await
        }
        pub async fn rerank(&self, query: &str, docs: &[String]) -> Result<Output, ApiError> {
            self.submit(|reply| Job::Rerank {
                query: query.into(),
                docs: docs.to_vec(),
                reply,
            })
            .await
        }

        /// The GGUF on disk: `path`, or the cached download from Hugging Face.
        async fn ensure_file(&self) -> Result<PathBuf, String> {
            if let Some(p) = &self.cfg.path {
                return Ok(p.into());
            }
            let repo = self.cfg.hf_repo.as_deref().expect("checked");
            let file = self.cfg.hf_file.as_deref().expect("checked");
            if file.contains("..") || repo.contains("..") {
                return Err("invalid hf_repo or hf_file".into());
            }
            let dest = self.options.cache_dir.join(repo).join(file);
            let marker = dest.with_extension("gguf.verified");
            if dest.exists() && marker.exists() {
                return Ok(dest);
            }
            download(&self.options, repo, file, &dest, self.cfg.sha256.as_deref()).await?;
            tokio::fs::write(&marker, b"ok")
                .await
                .map_err(|e| e.to_string())?;
            Ok(dest)
        }
    }

    async fn download(
        o: &ModelOptions,
        repo: &str,
        file: &str,
        dest: &Path,
        sha: Option<&str>,
    ) -> Result<(), String> {
        use tokio::io::AsyncWriteExt;
        let url = format!(
            "{}/{repo}/resolve/main/{file}",
            o.hf_endpoint.trim_end_matches('/')
        );
        let auth = |r: reqwest::RequestBuilder| match std::env::var("HF_TOKEN") {
            Ok(t) if !t.is_empty() => r.bearer_auth(t),
            _ => r,
        };
        // The hub's first response (before the CDN redirect) carries the file's size and SHA-256.
        let probe = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        let head = auth(probe.head(&url))
            .send()
            .await
            .map_err(|e| format!("cannot reach {}: {e}", o.hf_endpoint))?;
        let header = |n: &str| {
            head.headers()
                .get(n)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim_matches('"').to_string())
        };
        let want_size = header("x-linked-size")
            .or_else(|| header("content-length").filter(|_| head.status().is_success()))
            .and_then(|v| v.parse::<u64>().ok());
        let want_sha = sha.map(str::to_lowercase).or_else(|| {
            header("x-linked-etag")
                .filter(|e| e.len() == 64 && e.bytes().all(|b| b.is_ascii_hexdigit()))
        });
        if head.status().is_client_error() {
            return Err(format!(
                "{file} in {repo} was not found on the hub ({})",
                head.status()
            ));
        }
        tokio::fs::create_dir_all(dest.parent().expect("has parent"))
            .await
            .map_err(|e| e.to_string())?;
        let part = dest.with_extension("gguf.part");
        let mut resp = auth(reqwest::Client::new().get(&url))
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| format!("download failed: {e}"))?;
        let total = resp.content_length();
        let mut out = tokio::fs::File::create(&part)
            .await
            .map_err(|e| e.to_string())?;
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        tracing::info!(repo, file, bytes = ?total, "downloading local model");
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| format!("download failed: {e}"))?
        {
            hasher.update(&chunk);
            size += chunk.len() as u64;
            out.write_all(&chunk).await.map_err(|e| e.to_string())?;
        }
        out.flush().await.map_err(|e| e.to_string())?;
        drop(out);
        let digest = format!("{:x}", hasher.finalize());
        let bad = if total.is_some_and(|t| t != size) || want_size.is_some_and(|w| w != size) {
            Some(format!("size mismatch (got {size} bytes)"))
        } else if want_sha.as_ref().is_some_and(|w| *w != digest) {
            Some("sha256 mismatch".to_string())
        } else {
            None
        };
        if let Some(why) = bad {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(format!("downloaded {file} failed verification: {why}"));
        }
        tokio::fs::rename(&part, dest)
            .await
            .map_err(|e| e.to_string())?;
        tracing::info!(repo, file, size, sha256 = %digest, "local model downloaded");
        Ok(())
    }

    /// Load the model and spawn the worker threads.
    fn start(name: &str, task: Task, c: &ModelConfig, path: &Path) -> Result<Pool, String> {
        let backend = backend()?;
        let model = Arc::new(
            LlamaModel::load_from_file(backend, path, &LlamaModelParams::default())
                .map_err(|e| format!("cannot load {}: {e}", path.display()))?,
        );
        if task == Task::Embedding {
            let dims = c.dims.expect("validated");
            let native = model.n_embd_out() as usize;
            if native != dims {
                return Err(format!(
                    "the model produces {native} dimensions but dims = {dims}"
                ));
            }
        }
        let cores = std::thread::available_parallelism().map_or(2, usize::from);
        let threads = c.threads.unwrap_or((cores / 2).max(1)).max(1);
        let workers = c.workers.unwrap_or(1).clamp(1, threads);
        let per_worker = (threads / workers).max(1) as i32;
        let ctx_tokens = c.max_input_tokens.min(model.n_ctx_train() as usize).max(16);
        // Embedding sequences are packed into one decode. Rerank models are causal and keep a
        // KV cache that llama.cpp splits evenly between sequences, so they run one at a time.
        let seq_max = match task {
            Task::Embedding => c.batch_size.unwrap_or(32),
            Task::Rerank => 1,
        };
        let queue = c.queue.unwrap_or(64).max(1);
        let (tx, rx) = std::sync::mpsc::sync_channel::<Job>(queue);
        let rx = Arc::new(Mutex::new(rx));
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        for i in 0..workers {
            let (model, rx, ready) = (model.clone(), rx.clone(), ready_tx.clone());
            std::thread::Builder::new()
                .name(format!("bf-model-{name}-{i}"))
                .spawn(move || {
                    worker(
                        backend, &model, task, ctx_tokens, seq_max, per_worker, &rx, &ready,
                    )
                })
                .map_err(|e| e.to_string())?;
        }
        for _ in 0..workers {
            ready_rx.recv().map_err(|_| "worker exited".to_string())??;
        }
        tracing::info!(
            model = name,
            threads,
            workers,
            ctx_tokens,
            queue,
            "local model loaded"
        );
        Ok(Pool { tx })
    }

    #[allow(clippy::too_many_arguments)]
    fn worker(
        backend: &'static LlamaBackend,
        model: &LlamaModel,
        task: Task,
        ctx_tokens: usize,
        seq_max: usize,
        threads: i32,
        rx: &Mutex<Receiver<Job>>,
        ready: &std::sync::mpsc::Sender<Result<(), String>>,
    ) {
        let params = LlamaContextParams::default()
            .with_n_ctx(NonZeroU32::new(ctx_tokens as u32))
            .with_n_batch(ctx_tokens as u32)
            .with_n_ubatch(ctx_tokens as u32)
            .with_n_seq_max(seq_max as u32)
            .with_embeddings(true)
            .with_pooling_type(match task {
                Task::Embedding => LlamaPoolingType::Unspecified,
                Task::Rerank => LlamaPoolingType::Rank,
            })
            .with_n_threads(threads)
            .with_n_threads_batch(threads);
        let mut ctx = match model.new_context(backend, params) {
            Ok(c) => c,
            Err(e) => {
                let _ = ready.send(Err(format!("cannot create an inference context: {e}")));
                return;
            }
        };
        let _ = ready.send(Ok(()));
        let rerank_template = model
            .meta_val_str("tokenizer.chat_template.rerank", 8192)
            .ok();
        loop {
            let job = match rx.lock().expect("queue lock").recv() {
                Ok(j) => j,
                Err(_) => return,
            };
            match job {
                Job::Embed { texts, reply } => {
                    let seqs: Vec<Vec<LlamaToken>> = texts
                        .iter()
                        .map(|t| clip(tok(model, t, AddBos::Always, Special::Tokenize), ctx_tokens))
                        .collect();
                    let tokens = seqs.iter().map(Vec::len).sum::<usize>() as u64;
                    let out = pooled(&mut ctx, &seqs, ctx_tokens, seq_max).map(|mut vectors| {
                        for v in &mut vectors {
                            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                            if norm > 0.0 {
                                v.iter_mut().for_each(|x| *x /= norm);
                            }
                        }
                        Output { vectors, tokens }
                    });
                    let _ = reply.send(out);
                }
                Job::Rerank { query, docs, reply } => {
                    let seqs: Vec<Vec<LlamaToken>> = docs
                        .iter()
                        .map(|d| {
                            let t = match &rerank_template {
                                Some(t) => {
                                    let p = t.replace("{query}", &query).replace("{document}", d);
                                    tok(model, &p, AddBos::Always, Special::Tokenize)
                                }
                                None => {
                                    let mut t = vec![model.token_bos()];
                                    t.extend(tok(model, &query, AddBos::Never, Special::Plaintext));
                                    t.extend([model.token_eos(), model.token_sep()]);
                                    t.extend(tok(model, d, AddBos::Never, Special::Plaintext));
                                    t.push(model.token_eos());
                                    t
                                }
                            };
                            clip(t, ctx_tokens)
                        })
                        .collect();
                    let tokens = seqs.iter().map(Vec::len).sum::<usize>() as u64;
                    let out = pooled(&mut ctx, &seqs, ctx_tokens, seq_max)
                        .map(|vectors| Output { vectors, tokens });
                    let _ = reply.send(out);
                }
            }
        }
    }

    fn tok(model: &LlamaModel, text: &str, bos: AddBos, special: Special) -> Vec<LlamaToken> {
        let mut out = Vec::new();
        // Text containing NUL cannot reach the model; replace it rather than fail the batch.
        let text = text.replace('\0', " ");
        let _ = model.str_to_token_into_with_special(&text, bos, special, &mut out);
        out
    }

    /// Keep the sequence within the context, preserving its final (end-of-sequence) token.
    fn clip(mut t: Vec<LlamaToken>, max: usize) -> Vec<LlamaToken> {
        if t.len() > max {
            let last = *t.last().expect("nonempty");
            t.truncate(max - 1);
            t.push(last);
        }
        t
    }

    /// Run sequences through the model in packed batches, returning each one's pooled output.
    fn pooled(
        ctx: &mut LlamaContext<'_>,
        seqs: &[Vec<LlamaToken>],
        n_batch: usize,
        seq_max: usize,
    ) -> Result<Vec<Vec<f32>>, String> {
        let mut out = Vec::with_capacity(seqs.len());
        let mut i = 0;
        while i < seqs.len() {
            let (mut j, mut total) = (i, 0);
            while j < seqs.len() && j - i < seq_max && total + seqs[j].len().max(1) <= n_batch {
                total += seqs[j].len().max(1);
                j += 1;
            }
            if j == i {
                j = i + 1;
            }
            let mut batch = LlamaBatch::new(total.max(1), 1);
            for (k, s) in seqs[i..j].iter().enumerate() {
                if s.is_empty() {
                    return Err("empty input".into());
                }
                batch
                    .add_sequence(s, k as i32, false)
                    .map_err(|e| e.to_string())?;
            }
            ctx.clear_kv_cache();
            ctx.decode(&mut batch)
                .map_err(|e| format!("inference failed: {e}"))?;
            for k in 0..(j - i) {
                out.push(
                    ctx.embeddings_seq_ith(k as i32)
                        .map_err(|e| format!("no pooled output: {e}"))?
                        .to_vec(),
                );
            }
            i = j;
        }
        Ok(out)
    }
}
