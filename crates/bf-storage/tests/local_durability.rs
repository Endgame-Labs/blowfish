//! Linux syscall checks: exercise Storage, rather than merely the upstream backend's flag.
#![cfg(target_os = "linux")]
use bf_storage::{CreateResult, Storage, StorageConfig};
use std::{fs, path::Path, process::Command};
use uuid::Uuid;

fn scratch() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("local-durability-")
        .tempdir_in(fs::canonicalize(root).unwrap())
        .unwrap()
}

// Invoked in a separate process under strace, so the test runner's unrelated writes cannot
// satisfy the assertions. Markers are emitted only after Storage has acknowledged each write.
#[test]
#[ignore = "child of the syscall test"]
fn durability_child() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(durability_scenario());
}
async fn durability_scenario() {
    let root = std::env::var("BF_DURABILITY_ROOT").unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{root}/new/store"),
        ..Default::default()
    })
    .await
    .unwrap();
    println!("ACK:open");
    let id = Uuid::new_v4();
    assert_eq!(
        store
            .create_with_id("ns/a/wal/slot", "wal", id)
            .await
            .unwrap(),
        CreateResult::Created
    );
    println!("ACK:create");
    if std::env::var_os("BF_FAIL_DIRECTORY").is_some() {
        for n in 0..100 {
            let key = format!("ns/a/wal/failed-{n}");
            let id = Uuid::new_v4();
            if store.create_with_id(&key, "visible", id).await.is_err() {
                // Fail only after publication: the body is readable, but the directory
                // barrier failed both in the backend and in ambiguous-result recovery.
                assert_eq!(store.get(&key).await.unwrap(), "visible");
                assert!(store.create_with_id(&key, "visible", id).await.is_err());
                return;
            }
        }
        panic!("directory fsync injection did not reach a publication");
    }
    if std::env::var_os("BF_FAIL_RECOVERY").is_some() {
        // The parent injects EIO only when fsync targets the published file. The initial
        // staged-file fsync and directory publication succeeded, so the key remains visible.
        assert!(
            store
                .create_with_id("ns/a/wal/slot", "wal", id)
                .await
                .is_err()
        );
        assert!(
            store
                .redact_with_id("ns/a/wal/slot", "redacted", id)
                .await
                .unwrap()
        );
        assert!(
            store
                .redact_with_id("ns/a/wal/slot", "redacted", id)
                .await
                .is_err()
        );
        assert_eq!(store.get("ns/a/wal/slot").await.unwrap(), "redacted");
        return;
    }
    assert_eq!(
        store
            .create_with_id("ns/a/wal/slot", "wal", id)
            .await
            .unwrap(),
        CreateResult::AlreadyCommitted
    );
    println!("ACK:retry");
    assert_eq!(
        store.create("ns/a/wal/slot", "conflict").await.unwrap(),
        CreateResult::Conflict
    );
    store.put("ns/a/manifest/latest", "first").await.unwrap();
    println!("ACK:put");
    store.put("ns/a/manifest/latest", "second").await.unwrap();
    println!("ACK:overwrite");
    assert_eq!(
        store
            .create_unique_sized("ns/a/segments/data", vec![1; 6 << 20], 1, 5 << 20)
            .await
            .unwrap(),
        CreateResult::Created
    );
    println!("ACK:multipart");
    assert!(
        store
            .redact_with_id("ns/a/wal/slot", "redacted", id)
            .await
            .unwrap()
    );
    println!("ACK:redact");
    assert!(
        store
            .redact_with_id("ns/a/wal/slot", "redacted", id)
            .await
            .unwrap()
    );
    println!("ACK:redact_retry");
    drop(store);
    let read = Storage::open(&StorageConfig {
        url: format!("file://{root}/new/store"),
        read_only: true,
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(read.get("ns/a/wal/slot").await.unwrap(), "redacted");
    assert_eq!(read.get("ns/a/manifest/latest").await.unwrap(), "second");
    assert_eq!(read.get("ns/a/segments/data").await.unwrap().len(), 6 << 20);
    println!("ACK:reopen");
}

fn marker(lines: &[&str], name: &str) -> usize {
    lines
        .iter()
        .position(|line| line.contains(&format!("ACK:{name}\\n")))
        .unwrap_or_else(|| panic!("missing marker {name}"))
}
fn synced(line: &str, path: &str) -> bool {
    line.contains("fsync(") && line.contains(&format!("<{path}>")) && line.ends_with("= 0")
}

#[test]
fn local_publications_sync_files_and_directories_before_acknowledgment() {
    if Command::new("strace").arg("--version").output().is_err() {
        eprintln!("install strace to run the Linux durability syscall checks");
        return;
    }
    let run = scratch();
    let trace = run.path().join("trace");
    let result = Command::new("strace")
        .args([
            "-f",
            "-qq",
            "-yy",
            "-s",
            "4096",
            "-e",
            "trace=fsync,link,linkat,rename,renameat,renameat2,write",
            "-e",
            "status=successful",
        ])
        .arg("-o")
        .arg(&trace)
        .arg(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "durability_child", "--nocapture"])
        .env("BF_DURABILITY_ROOT", run.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let text = fs::read_to_string(trace).unwrap();
    let lines = text.lines().collect::<Vec<_>>();
    let root = format!("{}/new/store", run.path().display());
    let open = marker(&lines, "open");
    for dir in [
        run.path().display().to_string(),
        format!("{}/new", run.path().display()),
        root.clone(),
    ] {
        assert!(
            lines[..open].iter().any(|line| synced(line, &dir)),
            "store ancestor not synced: {dir}"
        );
    }
    let mut start = open;
    // The libc wrapper picks the syscall: x86_64 issues rename(2), while arm64 has only
    // renameat(2) and renameat2(2). Any of them publishes the file atomically.
    const LINK: &[&str] = &["linkat("];
    const RENAME: &[&str] = &["rename(", "renameat(", "renameat2("];
    for (name, key, publish) in [
        ("create", "ns/a/wal/slot", Some(LINK)),
        ("retry", "ns/a/wal/slot", None),
        ("put", "ns/a/manifest/latest", Some(RENAME)),
        ("overwrite", "ns/a/manifest/latest", Some(RENAME)),
        ("multipart", "ns/a/segments/data", Some(RENAME)),
        ("redact", "ns/a/wal/slot", Some(RENAME)),
        ("redact_retry", "ns/a/wal/slot", None),
    ] {
        let end = marker(&lines, name);
        let window = &lines[start + 1..end];
        let file = format!("{root}/{key}");
        let parent = Path::new(&file).parent().unwrap().display().to_string();
        if let Some(calls) = publish {
            let at = window
                .iter()
                .rposition(|line| {
                    calls.iter().any(|call| line.contains(call))
                        && line.contains(&format!("\"{file}\""))
                        && line.ends_with("= 0")
                })
                .unwrap_or_else(|| panic!("missing publication: {name}"));
            assert!(
                window[..at].iter().any(|line| line.contains("fsync(")
                    && line.contains(&format!("<{file}#"))
                    && line.ends_with("= 0")),
                "file not synced before {name}"
            );
            assert!(
                window[at + 1..].iter().any(|line| synced(line, &parent)),
                "directory not synced after {name}"
            );
        } else {
            let at = window
                .iter()
                .position(|line| synced(line, &file))
                .unwrap_or_else(|| panic!("recovered file not synced: {name}"));
            assert!(
                window[at + 1..].iter().any(|line| synced(line, &parent)),
                "recovered directory not synced: {name}"
            );
        }
        start = end;
    }
    // Each newly used ancestor is synced before it can expose the WAL slot.
    let create = marker(&lines, "create");
    for suffix in ["ns", "ns/a", "ns/a/wal"] {
        assert!(
            lines[open..create]
                .iter()
                .any(|line| synced(line, &format!("{root}/{suffix}")))
        );
    }
    let multipart = marker(&lines, "multipart");
    let reopen = marker(&lines, "reopen");
    assert!(
        lines[multipart..reopen]
            .iter()
            .any(|line| line.contains("ACK:redact"))
    );
    assert!(
        !lines[marker(&lines, "redact_retry") + 1..reopen]
            .iter()
            .any(|line| line.contains("fsync(")),
        "read-only open wrote to the store"
    );
}

#[test]
fn a_visible_retry_cannot_hide_a_failed_sync() {
    if Command::new("strace").arg("--version").output().is_err() {
        eprintln!("install strace to run the Linux durability fault checks");
        return;
    }
    let run = scratch();
    let trace = run.path().join("fault-trace");
    let result = Command::new("strace")
        .args([
            "-f",
            "-qq",
            "-yy",
            "-e",
            "trace=fsync",
            "-e",
            "inject=fsync:error=EIO",
        ])
        .arg("-P")
        .arg(run.path().join("new/store/ns/a/wal/slot"))
        .arg("-o")
        .arg(&trace)
        .arg(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "durability_child", "--nocapture"])
        .env("BF_DURABILITY_ROOT", run.path())
        .env("BF_FAIL_RECOVERY", "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let trace = fs::read_to_string(trace).unwrap();
    assert!(
        trace.matches("EIO (Input/output error) (INJECTED)").count() >= 2,
        "{trace}"
    );
}

#[test]
fn a_failed_publication_directory_sync_is_not_acknowledged() {
    if Command::new("strace").arg("--version").output().is_err() {
        eprintln!("install strace to run the Linux durability fault checks");
        return;
    }
    let run = scratch();
    let trace = run.path().join("directory-fault-trace");
    let result = Command::new("strace")
        .args([
            "-f",
            "-qq",
            "-yy",
            "-e",
            "trace=fsync",
            "-e",
            "inject=fsync:error=EIO:when=10+",
        ])
        .arg("-P")
        .arg(run.path().join("new/store/ns/a/wal"))
        .arg("-o")
        .arg(&trace)
        .arg(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "durability_child", "--nocapture"])
        .env("BF_DURABILITY_ROOT", run.path())
        .env("BF_FAIL_DIRECTORY", "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let trace = fs::read_to_string(trace).unwrap();
    assert!(
        trace.matches("EIO (Input/output error) (INJECTED)").count() >= 3,
        "{trace}"
    );
}
