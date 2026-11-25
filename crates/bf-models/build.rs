//! Link the archive omitted by llama-cpp-sys-4 0.12.0's CMake install step.
fn main() {
    if std::env::var_os("CARGO_FEATURE_LOCAL_MODELS").is_none() {
        return;
    }
    println!("cargo:rerun-if-env-changed=DEP_LLAMA_ROOT");
    let Some(root) = std::env::var_os("DEP_LLAMA_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let directory = root.join("build/vendor/cpp-httplib");
    // Upstream links installed lib*/ archives, but cpp-httplib has no install
    // directive. Its callers in llama-common require it even without model HTTP.
    // Do not add a second archive when a future upstream version installs it.
    if directory.join("libcpp-httplib.a").is_file()
        && !root.join("lib/libcpp-httplib.a").is_file()
        && !root.join("lib64/libcpp-httplib.a").is_file()
    {
        println!("cargo:rustc-link-search=native={}", directory.display());
        // BFD may encounter this archive before llama-common's references. Force
        // extraction so both BFD and LLD resolve it, regardless of archive order.
        println!("cargo:rustc-link-lib=static:+whole-archive=cpp-httplib");
    }
    // CMake's PRIVATE dependency edges do not reach Rust's static link. Forward
    // the TLS libraries too when this copy of cpp-httplib was built with OpenSSL.
    let cache = std::fs::read_to_string(root.join("build/CMakeCache.txt")).unwrap_or_default();
    if cache.lines().any(|line| line == "LLAMA_OPENSSL:BOOL=ON")
        && cache.lines().any(|line| {
            matches!(
                line,
                "OPENSSL_VERSION_SUPPORTED:INTERNAL=1" | "OPENSSL_VERSION_SUPPORTED:INTERNAL=TRUE"
            )
        })
    {
        for (key, name) in [
            ("OPENSSL_SSL_LIBRARY", "ssl"),
            ("OPENSSL_CRYPTO_LIBRARY", "crypto"),
        ] {
            let prefix = format!("{key}:FILEPATH=");
            let value = cache
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .expect("OpenSSL was enabled without its library path");
            let library = std::path::Path::new(value);
            let directory = library.parent().expect("OpenSSL library has no parent");
            let kind = if library.extension().is_some_and(|ext| ext == "a") {
                "static"
            } else {
                "dylib"
            };
            println!("cargo:rustc-link-search=native={}", directory.display());
            println!("cargo:rustc-link-lib={kind}={name}");
        }
        if std::env::var("CARGO_CFG_TARGET_VENDOR").as_deref() == Ok("apple") {
            println!("cargo:rustc-link-lib=framework=CoreFoundation");
            println!("cargo:rustc-link-lib=framework=Security");
        }
    }
}
