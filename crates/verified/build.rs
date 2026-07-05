fn main() {
    // `cargo creusot` compiles with --cfg creusot; register the cfg so the
    // workspace clippy gate (-D warnings, unexpected_cfgs) stays quiet on
    // stable builds.
    println!("cargo::rustc-check-cfg=cfg(creusot)");
}
