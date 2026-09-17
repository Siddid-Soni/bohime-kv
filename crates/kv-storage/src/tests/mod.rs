// `loom` needs the whole crate built with `--cfg loom` so that left-right's
// own atomics are loom's (see `loom_index.rs`), and loom's primitives panic
// outside a model — so under that cfg the loom model is the only test that
// builds. Everything else is the ordinary suite.
#[cfg(not(loom))]
mod config;
#[cfg(not(loom))]
mod engine;
#[cfg(not(loom))]
mod index;
#[cfg(not(loom))]
mod left_right;
#[cfg(loom)]
mod loom_index;
#[cfg(not(loom))]
mod record;
#[cfg(not(loom))]
mod scan;
