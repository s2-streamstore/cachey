#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod cache;
pub mod object_store;
pub mod service;
pub mod types;
