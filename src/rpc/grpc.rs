use crate::protos::raft::client_service_client::ClientServiceClient;
use crate::protos::raft::client_service_server::{ClientService, ClientServiceServer};
use crate::protos::raft::raft_service_client::RaftServiceClient;
use crate::protos::raft::raft_service_server::{RaftService, RaftServiceServer};
use crate::protos::raft::{
    command_request, log_entry, query_request, query_response, AppendEntriesRequest,
    AppendEntriesResponse, CommandRequest, CommandResponse, KvCommandPut, KvQueryGet,
    KvQueryGetResponse, QueryRequest, QueryResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::rpc::{ClientQueryResponse, RaftServer, RPC};
use crate::state::{KVCommand, KVQuery, KVQueryResponse};
use log::error;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tonic::transport::NamedService;
use tonic::{Code, Request, Response, Status};

pub async fn spawn_server(
    address: &str,
    server: Arc<RaftServer>,
) -> Result<impl RPC, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let address = SocketAddr::from_str(address)?;
    let raft_svc = RaftServiceServer::new(GrpcRaftService {
        server: server.clone(),
    });
    let client_svc = ClientServiceServer::new(GrpcClientService { server });
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(raft_svc)
            .add_service(client_svc)
            .serve(address)
            .await
        {
            error!("Server has errored: {}", e);
        }
    });
    Ok(clients::GrpcRPC::new())
}

#[derive(Clone)]
struct GrpcRaftService {
    server: Arc<RaftServer>,
}

impl NamedService for GrpcRaftService {
    const NAME: &'static str = "RaftService";
}

#[tonic::async_trait]
impl RaftService for GrpcRaftService {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let response = self
            .server
            .append_entries(crate::rpc::AppendEntriesRequest {
                leader_id: req.leader_id,
                term: req.term as usize,
                prev_log_index: req.prev_log_index as usize,
                prev_log_term: req.prev_log_term as usize,
                leader_commit: req.leader_commit as usize,
                entries: req
                    .entries
                    .into_iter()
                    .map(|l| crate::storage::LogEntry {
                        term: l.term as usize,
                        index: l.index as usize,
                        command: match l.command {
                            Some(log_entry::Command::KvPut(kv)) => {
                                crate::storage::LogCommand::Command(
                                    crate::state::KVCommand::Put {
                                        key: kv.key,
                                        value: kv.value.into(),
                                    }
                                    .into(),
                                )
                            }
                            _ => crate::storage::LogCommand::Noop,
                        },
                    })
                    .collect(),
            })
            .await
            .map(|res| AppendEntriesResponse {
                success: res.success,
                term: res.term as u64,
            });
        let msg = match response {
            Ok(r) => r,
            Err(e) => {
                error!("Failed while processing AppendEntries RPC: {}", e);
                AppendEntriesResponse {
                    success: false,
                    term: 0,
                }
            }
        };
        Ok(Response::new(msg))
    }
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let response = self
            .server
            .request_vote(crate::rpc::RequestVoteRequest {
                term: req.term as usize,
                candidate_id: req.candidate_id.clone(),
                last_log_index: req.last_log_index as usize,
                last_log_term: req.last_log_term as usize,
            })
            .await
            .map(|res| RequestVoteResponse {
                success: res.success,
                term: res.term as u64,
            });
        let msg = match response {
            Ok(r) => r,
            Err(e) => {
                error!("Failed while processing RequestVote RPC: {}", e);
                RequestVoteResponse {
                    success: false,
                    term: 0,
                }
            }
        };
        Ok(Response::new(msg))
    }
}

#[derive(Clone)]
struct GrpcClientService {
    server: Arc<RaftServer>,
}

impl NamedService for GrpcClientService {
    const NAME: &'static str = "ClientService";
}

#[tonic::async_trait]
impl ClientService for GrpcClientService {
    async fn command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        let response = self
            .server
            .apply(crate::client::Command::KV(
                match req.command {
                    Some(command_request::Command::KvPut(KvCommandPut { key, value })) => {
                        KVCommand::Put {
                            key,
                            value: value.into(),
                        }
                    }
                    _ => return Err(Status::invalid_argument("unknown command value")),
                }
                .into(),
            ))
            .await;
        match response {
            Ok(_) => Ok(Response::new(CommandResponse {})),
            Err(e) => {
                error!("Failed while processing Apply RPC: {}", e);
                Err(Status::new(Code::Internal, e.to_string()))
            }
        }
    }
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        let response = self
            .server
            .query(crate::client::Query::KV(
                match req.query {
                    Some(query_request::Query::KvGet(KvQueryGet { key })) => KVQuery::Get { key },
                    _ => return Err(Status::invalid_argument("unknown query value")),
                }
                .into(),
            ))
            .await
            .map(|res| QueryResponse {
                leader_address: res.leader_address.unwrap_or_else(|| String::new()),
                query: res.response.map(|qr| match qr {
                    crate::client::QueryResponse::KV(KVQueryResponse::Get { value }) => {
                        query_response::Query::KvGet(KvQueryGetResponse {
                            value: value.map(|v| v.to_vec()).unwrap_or_else(|| Vec::new()),
                        })
                    }
                }),
            });
        match response {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => {
                error!("Failed while processing Query RPC: {}", e);
                Err(Status::new(Code::Internal, e.to_string()))
            }
        }
    }
}

mod clients {
    use crate::client::{ClientError, RaftClient};
    use crate::protos::raft::client_service_client::ClientServiceClient;
    use crate::protos::raft::log_entry::Command;
    use crate::protos::raft::{
        command_request, query_request, raft_service_client::RaftServiceClient,
        AppendEntriesRequest, CommandRequest, CommandResponse, KvCommandPut, KvQueryGet,
        LogCommandKvPut, LogCommandNoop, LogEntry, QueryRequest, QueryResponse, RequestVoteRequest,
    };
    use crate::rpc::{AppendEntriesResponse, RequestVoteResponse, RPC};
    use crate::state::KVCommand;
    use async_lock::Lock;
    use std::collections::HashMap;
    use std::error::Error;
    use tonic::transport::Channel;
    use tonic::Status;

    #[derive(Clone)]
    pub struct GrpcRPC {
        clients: Lock<HashMap<String, RaftServiceClient<Channel>>>,
    }

    impl GrpcRPC {
        pub fn new() -> GrpcRPC {
            GrpcRPC {
                clients: Lock::new(HashMap::new()),
            }
        }

        async fn client(
            &self,
            peer_address: String,
        ) -> Result<RaftServiceClient<Channel>, Box<dyn Error + Send + Sync>> {
            let mut clients = self.clients.lock().await;
            if clients.contains_key(&peer_address) {
                Ok(clients.get(&peer_address).unwrap().clone())
            } else {
                let client = RaftServiceClient::connect(format!("http://{}", peer_address)).await?;
                clients.insert(peer_address, client.clone());
                Ok(client)
            }
        }
    }

    #[async_trait::async_trait]
    impl RPC for GrpcRPC {
        async fn append_entries(
            &self,
            peer_address: String,
            request: crate::rpc::AppendEntriesRequest,
        ) -> Result<crate::rpc::AppendEntriesResponse, Box<dyn Error + Send + Sync>> {
            let mut client = self.client(peer_address).await?;

            let request = tonic::Request::new(AppendEntriesRequest {
                leader_id: request.leader_id,
                term: request.term as u64,
                prev_log_index: request.prev_log_index as u64,
                prev_log_term: request.prev_log_term as u64,
                leader_commit: request.leader_commit as u64,
                entries: request
                    .entries
                    .into_iter()
                    .map(|l| LogEntry {
                        term: l.term as u64,
                        index: l.index as u64,
                        command: match l.command {
                            crate::storage::LogCommand::Noop => {
                                Some(Command::Noop(LogCommandNoop {}))
                            }
                            crate::storage::LogCommand::Command(crate::state::Command::KV(
                                KVCommand::Put { key, value },
                            )) => Some(Command::KvPut(LogCommandKvPut {
                                key,
                                value: value.to_vec(),
                            })),
                        },
                    })
                    .collect(),
            });

            let response = client
                .append_entries(request)
                .await
                .map(|r| r.into_inner())
                .map(|r| AppendEntriesResponse {
                    success: r.success,
                    term: r.term as usize,
                })?;
            Ok(response)
        }

        async fn request_vote(
            &self,
            peer_address: String,
            request: crate::rpc::RequestVoteRequest,
        ) -> Result<crate::rpc::RequestVoteResponse, Box<dyn Error + Send + Sync>> {
            let mut client = self.client(peer_address).await?;

            let request = tonic::Request::new(RequestVoteRequest {
                candidate_id: request.candidate_id,
                term: request.term as u64,
                last_log_term: request.last_log_term as u64,
                last_log_index: request.last_log_index as u64,
            });

            let response = client
                .request_vote(request)
                .await
                .map(|r| r.into_inner())
                .map(|r| RequestVoteResponse {
                    success: r.success,
                    term: r.term as usize,
                })?;
            Ok(response)
        }
    }

    #[derive(Clone)]
    pub struct GrpcClient {
        clients: Lock<ClientServiceClient<Channel>>,
    }

    impl GrpcClient {
        pub async fn new(address: String) -> Result<GrpcClient, tonic::transport::Error> {
            Ok(GrpcClient {
                clients: Lock::new(ClientServiceClient::connect(address).await?),
            })
        }

        async fn client(&self) -> ClientServiceClient<Channel> {
            self.clients.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl RaftClient for GrpcClient {
        async fn apply(&mut self, cmd: crate::state::Command) -> Result<(), ClientError> {
            let mut client = self.client().await;

            let request = tonic::Request::new(CommandRequest {
                command: Some(match cmd {
                    crate::state::Command::KV(KVCommand::Put { key, value }) => {
                        command_request::Command::KvPut(KvCommandPut {
                            key,
                            value: value.to_vec(),
                        })
                    }
                }),
            });

            let response = client.command(request).await.map_err(|e| {
                ClientError::IOError(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            Ok(())
        }

        async fn query(
            &mut self,
            query: crate::state::Query,
        ) -> Result<crate::state::QueryResponse, ClientError> {
            let mut client = self.client().await;

            let request = tonic::Request::new(QueryRequest {
                query: Some(match query {
                    crate::state::Query::KV(crate::state::KVQuery::Get { key }) => {
                        query_request::Query::KvGet(KvQueryGet { key })
                    }
                }),
            });

            // let response =
            //     client
            //         .query(request)
            //         .await
            //         .map(|r| r.into_inner())
            //         .map(|r| match r {
            //             QueryResponse::KV() => {
            //                 crate::state::QueryResponse::KV(crate::state::KVQueryResponse::Get {})
            //             }
            //         })?;
            // Ok(response)
            unimplemented!()
        }
    }
}
