use env_logger::Env;
use pontoon::{Client, ClientConfig};

pub fn spawn_three_servers() {
    pontoon::server("server1", "localhost:8001")
        .peer("server2", "localhost:8002")
        .peer("server3", "localhost:8003")
        .in_memory_storage()
        .spawn_http()
        .unwrap();
    pontoon::server("server2", "localhost:8002")
        .peer("server1", "localhost:8001")
        .peer("server3", "localhost:8003")
        .in_memory_storage()
        .spawn_http()
        .unwrap();
    pontoon::server("server3", "localhost:8003")
        .peer("server1", "localhost:8001")
        .peer("server2", "localhost:8002")
        .in_memory_storage()
        .spawn_http()
        .unwrap();
}

pub fn client(config: &'static str) -> impl Client {
    let config: ClientConfig = toml::from_str(config).unwrap();
    config.build().unwrap()
}

pub fn setup() {
    env_logger::from_env(Env::default())
        // .filter_level(log::LevelFilter::Debug)
        .filter_module("pontoon", log::LevelFilter::Trace)
        .format_timestamp_millis()
        .init();
}
