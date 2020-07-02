use env_logger::Env;
use pontoon::state::KeyValueStore;
use pontoon::{Config, Storage};

pub async fn state_machine(config: &'static str) -> KeyValueStore<impl Storage> {
    let config: Config = toml::from_str(config).unwrap();
    config.build_key_value_store().await.unwrap()
}

pub fn setup() {
    env_logger::from_env(Env::default())
        .filter_module("pontoon", log::LevelFilter::Debug)
        .init();
}
