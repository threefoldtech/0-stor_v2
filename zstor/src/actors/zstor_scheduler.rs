use std::collections::HashMap;
use std::path::Path;

use super::priority_queue::PriorityQueue;
use super::zstor::{Check, Rebuild, Retrieve, Store, StorePersist, ZstorActor, ZstorCommand};
use crate::{meta::Checksum, ZstorError, ZstorErrorKind};
use actix::prelude::*;
use log::{debug, error};
use rustbreak::backend::FileBackend;
use rustbreak::deser::Ron;
use rustbreak::{Database, FileDatabase, RustbreakError};
use tokio::sync::{mpsc, oneshot};
const LEVELS: u16 = 3;
use tokio::time;
use uuid::Uuid;

#[derive(Debug, Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// A message to schedule the given command
/// it receives the command priority and whether
/// the scheduler should block until the command is proocessed.
/// a response channel for pushing the reponse once the command is processed.
pub struct ZstorSchedulerMessage {
    cmd: ZstorSchedulerCommand,
    blocking: bool,
    priority: u16,
    response: oneshot::Sender<ZstorSchedulerResponse>,
}

#[derive(Debug, Clone)]
/// Scheduler command
pub enum ZstorSchedulerCommand {
    /// A zstor command
    ZstorCommand(ZstorCommand),
    /// This is used as a signal that the process is in termination state.
    /// It should return only after all pending operations are executed.
    Finalize,
}

#[derive(Debug, Message, Clone)]
#[rtype(result = "Result<(), ZstorError>")]
/// a message for the scheduler to indicate that the process
/// received a signal and is in termination state.
pub struct Signaled {}

#[derive(Debug)]
/// Responses for zstor scheduler messages.
/// It's a wrapper around the zstor responses except for Done.
pub enum ZstorSchedulerResponse {
    /// Response for check command
    Check(Result<Option<Checksum>, ZstorError>),
    /// Response for rebuild command
    Rebuild(Result<(), ZstorError>),
    /// Response for retrieve command
    Retrieve(Result<(), ZstorError>),
    /// Response for store command
    Store(Result<(), ZstorError>),
    /// Response for finalize command
    Done,
}
/// Worker for procesing the scheduler messages
struct Looper {
    zstor: Addr<ZstorActor>,
    cmds: PriorityQueue<ZstorSchedulerMessage>,
    ch: mpsc::UnboundedReceiver<ZstorSchedulerMessage>,
}

impl Looper {
    /// Receives the channel to which the scheduler messages are pushed.
    /// And the zstor responsible for the actual processing.
    pub fn new(
        ch: mpsc::UnboundedReceiver<ZstorSchedulerMessage>,
        zstor: Addr<ZstorActor>,
    ) -> Self {
        Looper {
            zstor,
            cmds: PriorityQueue::new(LEVELS),
            ch,
        }
    }
    async fn work(mut self) {
        loop {
            while let Ok(msg) = self.ch.try_recv() {
                debug!(
                    "scheduler received a command {:?} with prio {}",
                    msg, msg.priority
                );
                self.cmds.push(msg.priority, msg)
            }
            match self.cmds.pop() {
                Some(msg) => self.handle_msg(msg).await,
                None => {
                    if let Some(msg) = self.ch.recv().await {
                        debug!(
                            "scheduler received a command {:?} with prio {}",
                            msg, msg.priority
                        );
                        self.cmds.push(msg.priority, msg);
                    } else {
                        time::sleep(time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    async fn forward_cmd(&self, cmd: ZstorCommand) -> ZstorSchedulerResponse {
        match cmd {
            ZstorCommand::Store(store) => ZstorSchedulerResponse::Store(
                self.zstor
                    .send(store)
                    .await
                    .unwrap_or_else(|e| Err(ZstorError::from(e))),
            ),
            ZstorCommand::Retrieve(retrieve) => ZstorSchedulerResponse::Retrieve(
                self.zstor
                    .send(retrieve)
                    .await
                    .unwrap_or_else(|e| Err(ZstorError::from(e))),
            ),
            ZstorCommand::Rebuild(rebuild) => ZstorSchedulerResponse::Rebuild(
                self.zstor
                    .send(rebuild)
                    .await
                    .unwrap_or_else(|e| Err(ZstorError::from(e))),
            ),
            ZstorCommand::Check(check) => ZstorSchedulerResponse::Check(
                self.zstor
                    .send(check)
                    .await
                    .unwrap_or_else(|e| Err(ZstorError::from(e))),
            ),
        }
    }

    async fn handle_msg(&mut self, msg: ZstorSchedulerMessage) {
        match msg.cmd.clone() {
            ZstorSchedulerCommand::Finalize => {
                let _ = msg.response.send(ZstorSchedulerResponse::Done);
            }
            ZstorSchedulerCommand::ZstorCommand(cmd) => {
                debug!("the scheduler forwarding command {:?}", cmd);
                let res = self.forward_cmd(cmd.clone()).await;
                if !msg.blocking {
                    if let ZstorSchedulerResponse::Store(Err(e)) = res {
                        // - retry only if it's blocking, otherwise the sender is notified
                        // - retry indefinitely
                        error!("failed to process store command {}, queueing a retry", e);
                        self.cmds.repush(msg.priority, msg);
                    }
                } else {
                    // error means the receiver hung up, should log probably, but shouldn't happen anyway
                    let _ = msg.response.send(res);
                }
            }
        }
    }
}

/// This DB is works in-memory until calling save() function
pub struct StoreDb {
    db: Database<HashMap<u128, Store>, FileBackend, Ron>,
}

impl StoreDb {
    /// Create a new StoreDb
    /// if the path exists load it
    pub fn new_or_load_from<P: AsRef<Path>>(path: P) -> Self {
        let db = FileDatabase::<HashMap<u128, Store>, Ron>::load_from_path_or_default(path)
            .expect("can't open database file for saving stores");

        Self { db }
    }
}

impl StorePersist for StoreDb {
    type Error = RustbreakError;

    fn insert(&self, store: Store) -> u128 {
        let id = Uuid::new_v4().as_u128();
        let result = self.db.write(|db| {
            db.insert(id, store);
        });
        if let Err(err) = result {
            log::debug!("failed to insert store into store db: '{}'", err);
        }
        id
    }

    fn delete(&self, id: u128) {
        let result = self.db.write(|db| db.remove(&id));

        if let Err(err) = result {
            log::debug!("failed to remove store from store db: '{}'", err);
        }
    }

    fn save(&self) -> Result<(), Self::Error> {
        self.db.save()
    }

    fn vectored_content(&self) -> Vec<Store> {
        let data = self
            .db
            .read(|db| {
                let stores = { db.values().into_iter().map(|v| v.to_owned()).collect() };
                stores
            })
            .unwrap();

        self.db
            .write(|db| {
                db.clear();
            })
            .unwrap();

        data
    }
}

/// Actor for the main zstor object encoding and decoding.
pub struct ZstorActorScheduler<D> {
    zstor: Addr<ZstorActor>,
    ch: Option<mpsc::UnboundedSender<ZstorSchedulerMessage>>,
    stores: D,
}

impl<D> ZstorActorScheduler<D>
where
    D: StorePersist,
{
    /// new
    pub fn new(zstor: Addr<ZstorActor>, store_db: D) -> ZstorActorScheduler<D> {
        Self {
            zstor,
            ch: None,
            stores: store_db,
        }
    }

    fn push_zstor(
        ch: mpsc::UnboundedSender<ZstorSchedulerMessage>,
        cmd: ZstorCommand,
        blocking: bool,
    ) -> Result<oneshot::Receiver<ZstorSchedulerResponse>, ZstorError> {
        Self::push(
            ch,
            ZstorSchedulerCommand::ZstorCommand(cmd),
            blocking,
            if blocking { 0 } else { 1 },
        )
    }

    fn push(
        ch: mpsc::UnboundedSender<ZstorSchedulerMessage>,
        cmd: ZstorSchedulerCommand,
        blocking: bool,
        priority: u16,
    ) -> Result<oneshot::Receiver<ZstorSchedulerResponse>, ZstorError> {
        let (tx, rx) = oneshot::channel();
        let msg = ZstorSchedulerMessage {
            cmd,
            blocking,
            priority,
            response: tx,
        };
        ch.send(msg)
            .map_err(|err| ZstorError::with_message(ZstorErrorKind::Channel, err.to_string()))?;
        Ok(rx)
    }
}

impl<D> Actor for ZstorActorScheduler<D>
where
    D: std::marker::Unpin + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.ch = Some(tx);
        let looper = Looper::new(rx, self.zstor.clone());
        ctx.spawn(looper.work().into_actor(self));
    }
}

impl<D> Handler<Store> for ZstorActorScheduler<D>
where
    D: 'static + StorePersist + std::marker::Unpin,
{
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, msg: Store, _: &mut Self::Context) -> Self::Result {
        let ch = self.ch.as_ref().unwrap().clone();

        let id = self.stores.insert(msg.clone());

        let ret = Box::pin(async move {
            let blocking = msg.blocking;
            let resp_fut = Self::push_zstor(ch, ZstorCommand::Store(msg), blocking)?;
            if !blocking {
                return Ok(());
            }
            let resp = resp_fut.await.map_err(|err| {
                ZstorError::with_message(ZstorErrorKind::Channel, err.to_string())
            })?;
            match resp {
                ZstorSchedulerResponse::Store(v) => v,
                _ => Err(ZstorError::with_message(
                    ZstorErrorKind::Channel,
                    format!("received {:?} while expecting a store response", resp),
                )),
            }
        });

        self.stores.delete(id);

        ret
    }
}

impl<D> Handler<Retrieve> for ZstorActorScheduler<D>
where
    D: 'static + StorePersist + std::marker::Unpin,
{
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, msg: Retrieve, _: &mut Self::Context) -> Self::Result {
        let ch = self.ch.as_ref().unwrap().clone();
        Box::pin(async move {
            let resp = Self::push_zstor(ch, ZstorCommand::Retrieve(msg), true)?
                .await
                .map_err(|err| {
                    ZstorError::with_message(ZstorErrorKind::Channel, err.to_string())
                })?;
            match resp {
                ZstorSchedulerResponse::Retrieve(v) => v,
                _ => Err(ZstorError::with_message(
                    ZstorErrorKind::Channel,
                    format!("received {:?} while expecting a store response", resp),
                )),
            }
        })
    }
}

impl<D> Handler<Rebuild> for ZstorActorScheduler<D>
where
    D: 'static + StorePersist + std::marker::Unpin,
{
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, msg: Rebuild, _: &mut Self::Context) -> Self::Result {
        let ch = self.ch.as_ref().unwrap().clone();
        Box::pin(async move {
            let resp = Self::push_zstor(ch, ZstorCommand::Rebuild(msg), true)?
                .await
                .map_err(|err| {
                    ZstorError::with_message(ZstorErrorKind::Channel, err.to_string())
                })?;
            match resp {
                ZstorSchedulerResponse::Rebuild(v) => v,
                _ => Err(ZstorError::with_message(
                    ZstorErrorKind::Channel,
                    format!("received {:?} while expecting a rebuild response", resp),
                )),
            }
        })
    }
}

impl<D> Handler<Check> for ZstorActorScheduler<D>
where
    D: 'static + StorePersist + std::marker::Unpin,
{
    type Result = ResponseFuture<Result<Option<Checksum>, ZstorError>>;

    fn handle(&mut self, msg: Check, _: &mut Self::Context) -> Self::Result {
        let ch = self.ch.as_ref().unwrap().clone();
        Box::pin(async move {
            let resp = Self::push_zstor(ch, ZstorCommand::Check(msg), true)?
                .await
                .map_err(|err| {
                    ZstorError::with_message(ZstorErrorKind::Channel, err.to_string())
                })?;
            match resp {
                ZstorSchedulerResponse::Check(v) => v,
                _ => Err(ZstorError::with_message(
                    ZstorErrorKind::Channel,
                    format!("received {:?} while expecting a check response", resp),
                )),
            }
        })
    }
}

impl<D> Handler<Signaled> for ZstorActorScheduler<D>
where
    D: 'static + StorePersist + std::marker::Unpin,
{
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, _: Signaled, _: &mut Self::Context) -> Self::Result {
        let ch = self.ch.as_ref().unwrap().clone();
        let ret = Box::pin(async move {
            let resp = Self::push(ch, ZstorSchedulerCommand::Finalize, true, LEVELS - 1)?
                .await
                .map_err(|err| {
                    ZstorError::with_message(ZstorErrorKind::Channel, err.to_string())
                })?;
            match resp {
                ZstorSchedulerResponse::Done => Ok(()),
                _ => Err(ZstorError::with_message(
                    ZstorErrorKind::Channel,
                    format!("received {:?} while expecting a check response", resp),
                )),
            }
        });


        if let Err(_) = self.stores.save() {
            error!("Error while saving non-handled stores");
        }

        ret
    }
}
