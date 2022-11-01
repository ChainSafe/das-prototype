use discv5::Enr;
use enr::k256::elliptic_curve::weierstrass::add;
use enr::NodeId;
use eyre::anyhow;
use futures::future::BoxFuture;
use futures::SinkExt;
use rocket::http::Status;
use rocket::response::status;
use rocket::serde::{json::Json, Deserialize, Serialize};
use rocket::{routes, State};
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};

pub enum RpcMsg {
    TalkReq(Vec<u8>, oneshot::Sender<Vec<u8>>),
}

struct Runtime {
    tx: mpsc::Sender<RpcMsg>,
}

#[post("/talk_req", data = "<req>")]
async fn api_talk_req(
    state: &State<Runtime>,
    req: Vec<u8>,
) -> Result<Vec<u8>, status::Custom<String>> {
    let (tx, rx) = oneshot::channel();

    state
        .tx
        .clone()
        .send(RpcMsg::TalkReq(req, tx))
        .await
        .map_err(|e| status::Custom(Status::ServiceUnavailable, e.to_string()))?;

    let res = rx
        .await
        .map_err(|e| status::Custom(Status::InternalServerError, e.to_string()))?;

    Ok(res)
}

#[allow(unused_must_use)]
pub async fn serve(to_runtime: mpsc::Sender<RpcMsg>, addr: impl Into<SocketAddr>) {
    let addr = addr.into();
    let mut config = rocket::Config::default();
    config.address = addr.ip();
    config.port = addr.port();
    config.shutdown.ctrlc = true;
    config.shutdown.force = true;

    rocket::custom(config)
        .manage(Runtime { tx: to_runtime })
        .mount("/", routes![api_talk_req])
        .launch()
        .await
        .expect("expect server to run");
}

pub async fn talk_req(enr: Enr, msg: Vec<u8>) -> eyre::Result<Vec<u8>> {
    let addr = enr.ip4().unwrap().to_string();
    let port = 3000 + (enr.udp4().unwrap() - 9000); // todo: dirty deterministic hack, should be config instead
    let client = reqwest::Client::new();

    let mut resp = client
        .post(format!("http://{addr}:{port}/talk_req"))
        .body(msg)
        .send()
        .await
        .map_err(|e| anyhow!("error requesting setup: {e}"))?;

    if resp.status() != 200 {
        return Err(anyhow!("error status: {}", resp.status()));
    }

    resp.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| anyhow!("error decoding step1 response: {e}"))
}
