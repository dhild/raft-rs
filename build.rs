fn main() {
    #[cfg(feature = "grpc-rpc")]
    tonic_build::compile_protos("src/protos/raft.proto").expect("tonic build");
}
