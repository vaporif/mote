fn main() {
    #[cfg(feature = "grpc")]
    tonic_prost_build::compile_protos("proto/exex.proto").expect("failed to compile exex.proto");
}
