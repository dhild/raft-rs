#[macro_use]
extern crate clap;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate raft_rs;

use clap::{App, ArgMatches};
use env_logger::Builder;
use log::LevelFilter;

fn main() {
    ::std::process::exit(main_code())
}

fn main_code() -> i32 {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml)
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .get_matches();

    setup_logging(&matches);

    let mut config = raft_rs::RaftConfig::default();
    if let Some(addr) = matches.value_of("address") {
        match addr.parse() {
            Ok(addr) => config.server_addr = addr,
            Err(e) => {
                error!("Could not parse server address: {}", e);
                return 1;
            }
        };
    }
    if let Some(peers) = matches.values_of("peer") {
        for addr in peers {
            match addr.parse() {
                Ok(addr) => config.peers.push(addr),
                Err(e) => {
                    error!("Could not parse peer address '{}': {}", addr, e);
                    return 1;
                }
            };
        }
    }

    match matches.subcommand() {
        ("serve", Some(_serve_matches)) => raft_rs::serve(config),
        _ => {
            error!("No subcommand matched!");
            return 2;
        }
    };
    0
}

fn setup_logging(matches: &ArgMatches) {
    let mut builder = Builder::from_default_env();
    match matches.occurrences_of("verbose") {
        0 => builder.filter_level(LevelFilter::Warn),
        1 => builder.filter_level(LevelFilter::Info),
        2 => builder.filter_level(LevelFilter::Debug),
        _ => builder.filter_level(LevelFilter::Trace),
    };
    builder.init();
}
