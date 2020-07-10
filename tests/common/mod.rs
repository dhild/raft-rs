use env_logger::Env;
use pontoon::{Client, ClientConfig, ServerConfig};

pub async fn state_machine(config: &'static str) {
    let config: ServerConfig = toml::from_str(config).unwrap();
    config.spawn_server().await.unwrap();
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
