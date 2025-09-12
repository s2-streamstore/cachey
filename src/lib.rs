#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod cache;
pub mod object_store;
pub mod service;
pub mod types;
