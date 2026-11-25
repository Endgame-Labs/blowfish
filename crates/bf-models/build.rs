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
}
