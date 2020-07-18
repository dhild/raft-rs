pub mod raft {
    // #[cfg(feature = "grpc-rpc")]
    // tonic::include_proto!("raft");
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AppendEntriesRequest {
        #[prost(string, tag = "1")]
        pub leader_id: std::string::String,
        #[prost(uint64, tag = "2")]
        pub term: u64,
        #[prost(uint64, tag = "3")]
        pub prev_log_index: u64,
        #[prost(uint64, tag = "4")]
        pub prev_log_term: u64,
        #[prost(uint64, tag = "5")]
        pub leader_commit: u64,
        #[prost(message, repeated, tag = "6")]
        pub entries: ::std::vec::Vec<LogEntry>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct LogEntry {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(uint64, tag = "2")]
        pub index: u64,
        #[prost(oneof = "log_entry::Command", tags = "3, 4")]
        pub command: ::std::option::Option<log_entry::Command>,
    }
    pub mod log_entry {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Command {
            #[prost(message, tag = "3")]
            Noop(super::LogCommandNoop),
            #[prost(message, tag = "4")]
            KvPut(super::LogCommandKvPut),
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct LogCommandNoop {}
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct LogCommandKvPut {
        #[prost(string, tag = "1")]
        pub key: std::string::String,
        #[prost(bytes, tag = "2")]
        pub value: std::vec::Vec<u8>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AppendEntriesResponse {
        #[prost(bool, tag = "1")]
        pub success: bool,
        #[prost(uint64, tag = "2")]
        pub term: u64,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RequestVoteRequest {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(string, tag = "2")]
        pub candidate_id: std::string::String,
        #[prost(uint64, tag = "3")]
        pub last_log_index: u64,
        #[prost(uint64, tag = "4")]
        pub last_log_term: u64,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RequestVoteResponse {
        #[prost(bool, tag = "1")]
        pub success: bool,
        #[prost(uint64, tag = "2")]
        pub term: u64,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CommandRequest {
        #[prost(oneof = "command_request::Command", tags = "1")]
        pub command: ::std::option::Option<command_request::Command>,
    }
    pub mod command_request {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Command {
            #[prost(message, tag = "1")]
            KvPut(super::KvCommandPut),
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KvCommandPut {
        #[prost(string, tag = "1")]
        pub key: std::string::String,
        #[prost(bytes, tag = "2")]
        pub value: std::vec::Vec<u8>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CommandResponse {}
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct QueryRequest {
        #[prost(oneof = "query_request::Query", tags = "1")]
        pub query: ::std::option::Option<query_request::Query>,
    }
    pub mod query_request {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Query {
            #[prost(message, tag = "1")]
            KvGet(super::KvQueryGet),
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KvQueryGet {
        #[prost(string, tag = "1")]
        pub key: std::string::String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct QueryResponse {
        #[prost(string, tag = "1")]
        pub leader_address: std::string::String,
        #[prost(oneof = "query_response::Query", tags = "2")]
        pub query: ::std::option::Option<query_response::Query>,
    }
    pub mod query_response {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Query {
            #[prost(message, tag = "2")]
            KvGet(super::KvQueryGetResponse),
        }
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct KvQueryGetResponse {
        #[prost(bytes, tag = "1")]
        pub value: std::vec::Vec<u8>,
    }
    #[doc = r" Generated client implementations."]
    pub mod raft_service_client {
        #![allow(unused_variables, dead_code, missing_docs)]
        use tonic::codegen::*;
        pub struct RaftServiceClient<T> {
            inner: tonic::client::Grpc<T>,
        }
        impl RaftServiceClient<tonic::transport::Channel> {
            #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
            pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
            where
                D: std::convert::TryInto<tonic::transport::Endpoint>,
                D::Error: Into<StdError>,
            {
                let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
                Ok(Self::new(conn))
            }
        }
        impl<T> RaftServiceClient<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: Body + HttpBody + Send + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
        {
            pub fn new(inner: T) -> Self {
                let inner = tonic::client::Grpc::new(inner);
                Self { inner }
            }
            pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
                let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
                Self { inner }
            }
            pub async fn append_entries(
                &mut self,
                request: impl tonic::IntoRequest<super::AppendEntriesRequest>,
            ) -> Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status> {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/raft.RaftService/AppendEntries");
                self.inner.unary(request.into_request(), path, codec).await
            }
            pub async fn request_vote(
                &mut self,
                request: impl tonic::IntoRequest<super::RequestVoteRequest>,
            ) -> Result<tonic::Response<super::RequestVoteResponse>, tonic::Status> {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/raft.RaftService/RequestVote");
                self.inner.unary(request.into_request(), path, codec).await
            }
        }
        impl<T: Clone> Clone for RaftServiceClient<T> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }
        impl<T> std::fmt::Debug for RaftServiceClient<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "RaftServiceClient {{ ... }}")
            }
        }
    }
    #[doc = r" Generated client implementations."]
    pub mod client_service_client {
        #![allow(unused_variables, dead_code, missing_docs)]
        use tonic::codegen::*;
        pub struct ClientServiceClient<T> {
            inner: tonic::client::Grpc<T>,
        }
        impl ClientServiceClient<tonic::transport::Channel> {
            #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
            pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
            where
                D: std::convert::TryInto<tonic::transport::Endpoint>,
                D::Error: Into<StdError>,
            {
                let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
                Ok(Self::new(conn))
            }
        }
        impl<T> ClientServiceClient<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: Body + HttpBody + Send + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
        {
            pub fn new(inner: T) -> Self {
                let inner = tonic::client::Grpc::new(inner);
                Self { inner }
            }
            pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
                let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
                Self { inner }
            }
            pub async fn command(
                &mut self,
                request: impl tonic::IntoRequest<super::CommandRequest>,
            ) -> Result<tonic::Response<super::CommandResponse>, tonic::Status> {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/raft.ClientService/Command");
                self.inner.unary(request.into_request(), path, codec).await
            }
            pub async fn query(
                &mut self,
                request: impl tonic::IntoRequest<super::QueryRequest>,
            ) -> Result<tonic::Response<super::QueryResponse>, tonic::Status> {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/raft.ClientService/Query");
                self.inner.unary(request.into_request(), path, codec).await
            }
        }
        impl<T: Clone> Clone for ClientServiceClient<T> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }
        impl<T> std::fmt::Debug for ClientServiceClient<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "ClientServiceClient {{ ... }}")
            }
        }
    }
    #[doc = r" Generated server implementations."]
    pub mod raft_service_server {
        #![allow(unused_variables, dead_code, missing_docs)]
        use tonic::codegen::*;
        #[doc = "Generated trait containing gRPC methods that should be implemented for use with RaftServiceServer."]
        #[async_trait]
        pub trait RaftService: Send + Sync + 'static {
            async fn append_entries(
                &self,
                request: tonic::Request<super::AppendEntriesRequest>,
            ) -> Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status>;
            async fn request_vote(
                &self,
                request: tonic::Request<super::RequestVoteRequest>,
            ) -> Result<tonic::Response<super::RequestVoteResponse>, tonic::Status>;
        }
        #[derive(Debug)]
        pub struct RaftServiceServer<T: RaftService> {
            inner: _Inner<T>,
        }
        struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
        impl<T: RaftService> RaftServiceServer<T> {
            pub fn new(inner: T) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner, None);
                Self { inner }
            }
            pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner, Some(interceptor.into()));
                Self { inner }
            }
        }
        impl<T, B> Service<http::Request<B>> for RaftServiceServer<T>
        where
            T: RaftService,
            B: HttpBody + Send + Sync + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Never;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                let inner = self.inner.clone();
                match req.uri().path() {
                    "/raft.RaftService/AppendEntries" => {
                        #[allow(non_camel_case_types)]
                        struct AppendEntriesSvc<T: RaftService>(pub Arc<T>);
                        impl<T: RaftService>
                            tonic::server::UnaryService<super::AppendEntriesRequest>
                            for AppendEntriesSvc<T>
                        {
                            type Response = super::AppendEntriesResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::AppendEntriesRequest>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).append_entries(request).await };
                                Box::pin(fut)
                            }
                        }
                        let inner = self.inner.clone();
                        let fut = async move {
                            let interceptor = inner.1.clone();
                            let inner = inner.0;
                            let method = AppendEntriesSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = if let Some(interceptor) = interceptor {
                                tonic::server::Grpc::with_interceptor(codec, interceptor)
                            } else {
                                tonic::server::Grpc::new(codec)
                            };
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    "/raft.RaftService/RequestVote" => {
                        #[allow(non_camel_case_types)]
                        struct RequestVoteSvc<T: RaftService>(pub Arc<T>);
                        impl<T: RaftService> tonic::server::UnaryService<super::RequestVoteRequest> for RequestVoteSvc<T> {
                            type Response = super::RequestVoteResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::RequestVoteRequest>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).request_vote(request).await };
                                Box::pin(fut)
                            }
                        }
                        let inner = self.inner.clone();
                        let fut = async move {
                            let interceptor = inner.1.clone();
                            let inner = inner.0;
                            let method = RequestVoteSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = if let Some(interceptor) = interceptor {
                                tonic::server::Grpc::with_interceptor(codec, interceptor)
                            } else {
                                tonic::server::Grpc::new(codec)
                            };
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(200)
                            .header("grpc-status", "12")
                            .body(tonic::body::BoxBody::empty())
                            .unwrap())
                    }),
                }
            }
        }
        impl<T: RaftService> Clone for RaftServiceServer<T> {
            fn clone(&self) -> Self {
                let inner = self.inner.clone();
                Self { inner }
            }
        }
        impl<T: RaftService> Clone for _Inner<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone(), self.1.clone())
            }
        }
        impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }
        impl<T: RaftService> tonic::transport::NamedService for RaftServiceServer<T> {
            const NAME: &'static str = "raft.RaftService";
        }
    }
    #[doc = r" Generated server implementations."]
    pub mod client_service_server {
        #![allow(unused_variables, dead_code, missing_docs)]
        use tonic::codegen::*;
        #[doc = "Generated trait containing gRPC methods that should be implemented for use with ClientServiceServer."]
        #[async_trait]
        pub trait ClientService: Send + Sync + 'static {
            async fn command(
                &self,
                request: tonic::Request<super::CommandRequest>,
            ) -> Result<tonic::Response<super::CommandResponse>, tonic::Status>;
            async fn query(
                &self,
                request: tonic::Request<super::QueryRequest>,
            ) -> Result<tonic::Response<super::QueryResponse>, tonic::Status>;
        }
        #[derive(Debug)]
        pub struct ClientServiceServer<T: ClientService> {
            inner: _Inner<T>,
        }
        struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
        impl<T: ClientService> ClientServiceServer<T> {
            pub fn new(inner: T) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner, None);
                Self { inner }
            }
            pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner, Some(interceptor.into()));
                Self { inner }
            }
        }
        impl<T, B> Service<http::Request<B>> for ClientServiceServer<T>
        where
            T: ClientService,
            B: HttpBody + Send + Sync + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Never;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                let inner = self.inner.clone();
                match req.uri().path() {
                    "/raft.ClientService/Command" => {
                        #[allow(non_camel_case_types)]
                        struct CommandSvc<T: ClientService>(pub Arc<T>);
                        impl<T: ClientService> tonic::server::UnaryService<super::CommandRequest> for CommandSvc<T> {
                            type Response = super::CommandResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::CommandRequest>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).command(request).await };
                                Box::pin(fut)
                            }
                        }
                        let inner = self.inner.clone();
                        let fut = async move {
                            let interceptor = inner.1.clone();
                            let inner = inner.0;
                            let method = CommandSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = if let Some(interceptor) = interceptor {
                                tonic::server::Grpc::with_interceptor(codec, interceptor)
                            } else {
                                tonic::server::Grpc::new(codec)
                            };
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    "/raft.ClientService/Query" => {
                        #[allow(non_camel_case_types)]
                        struct QuerySvc<T: ClientService>(pub Arc<T>);
                        impl<T: ClientService> tonic::server::UnaryService<super::QueryRequest> for QuerySvc<T> {
                            type Response = super::QueryResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::QueryRequest>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).query(request).await };
                                Box::pin(fut)
                            }
                        }
                        let inner = self.inner.clone();
                        let fut = async move {
                            let interceptor = inner.1.clone();
                            let inner = inner.0;
                            let method = QuerySvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = if let Some(interceptor) = interceptor {
                                tonic::server::Grpc::with_interceptor(codec, interceptor)
                            } else {
                                tonic::server::Grpc::new(codec)
                            };
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(200)
                            .header("grpc-status", "12")
                            .body(tonic::body::BoxBody::empty())
                            .unwrap())
                    }),
                }
            }
        }
        impl<T: ClientService> Clone for ClientServiceServer<T> {
            fn clone(&self) -> Self {
                let inner = self.inner.clone();
                Self { inner }
            }
        }
        impl<T: ClientService> Clone for _Inner<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone(), self.1.clone())
            }
        }
        impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }
        impl<T: ClientService> tonic::transport::NamedService for ClientServiceServer<T> {
            const NAME: &'static str = "raft.ClientService";
        }
    }
}
