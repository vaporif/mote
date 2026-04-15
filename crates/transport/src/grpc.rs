// We use a small custom proto here instead of Arrow Flight. Flight assumes a
// query-server model where clients request data; we have a one-way push stream
// from node to sidecar. The proto is trivial (~15 lines) and wrapping Arrow IPC
// bytes in protobuf costs nothing — protobuf just passes them through as a blob.

pub use crate::grpc_client::GrpcClient;
pub use crate::grpc_server::GrpcServer;
