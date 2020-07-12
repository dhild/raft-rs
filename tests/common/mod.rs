use env_logger::Env;

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

pub fn setup() {
    env_logger::from_env(Env::default())
        // .filter_level(log::LevelFilter::Debug)
        .filter_module("pontoon", log::LevelFilter::Trace)
        .format_timestamp_millis()
        .init();
}
