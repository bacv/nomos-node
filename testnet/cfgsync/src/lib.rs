pub mod config;
pub mod repo;

#[derive(Clone)]
pub struct TracingConfig {
    pub trace_endpoint: String,
    pub log_endpoint: String,
}
