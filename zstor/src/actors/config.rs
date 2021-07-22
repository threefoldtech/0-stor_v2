use crate::{
    config::{Config, Meta},
    zdb::ZdbConnectionInfo,
    ZstorError,
};
use actix::prelude::*;
use log::error;
use std::{convert::TryInto, ops::Deref, path::PathBuf, sync::Arc};
use tokio::fs;

#[derive(Message)]
#[rtype(result = "Arc<Config>")]
/// Message to request the current config
pub struct GetConfig;

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message to trigger a config reload
pub struct ReloadConfig;

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message to add a new 0-db to the configuration. The group in which to add the 0-db must be
/// supplied. It is possible to instruct the removal of an old 0-db as well, though this is not
/// required.
pub struct AddZdb {
    /// Group index in which to add the new 0-db
    pub group_idx: usize,
    /// [`ZdbConnectionInfo`] of the new 0-db.
    pub ci: ZdbConnectionInfo,
    /// [`ZdbConnectionInfo`] to identify the 0-db which needs to be removed.
    pub replaced: Option<ZdbConnectionInfo>,
}

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message to replace the 0-db instances in a 0-db metastore backend.
pub struct ReplaceMetaBackend {
    /// Connection info for all the nodes making up the new metadata backend.
    pub new_nodes: Vec<ZdbConnectionInfo>,
}

/// Actor holding the configuration, and the path where it is saved.
pub struct ConfigActor {
    config_path: PathBuf,
    config: Arc<Config>,
}

impl ConfigActor {
    /// Create a new config actor.
    pub fn new(config_path: PathBuf, config: Config) -> ConfigActor {
        Self {
            config_path,
            config: Arc::new(config),
        }
    }
}

impl Actor for ConfigActor {
    type Context = Context<Self>;
}

impl Handler<GetConfig> for ConfigActor {
    type Result = Arc<Config>;

    fn handle(&mut self, _: GetConfig, _: &mut Self::Context) -> Self::Result {
        self.config.clone()
    }
}

impl Handler<ReloadConfig> for ConfigActor {
    type Result = AtomicResponse<Self, Result<(), ZstorError>>;

    fn handle(&mut self, _: ReloadConfig, _: &mut Self::Context) -> Self::Result {
        let path = self.config_path.clone();
        AtomicResponse::new(Box::pin(
            async move {
                fs::read(path)
                    .await
                    .map_err(|e| ZstorError::new_io("Couldn't load config file".to_string(), e))
            }
            .into_actor(self)
            .map(|res, this, _| {
                let config = toml::from_slice(&res?)?;
                this.config = Arc::new(config);
                Ok(())
            }),
        ))
    }
}

impl Handler<AddZdb> for ConfigActor {
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, msg: AddZdb, _: &mut Self::Context) -> Self::Result {
        let mut new_config = Arc::clone(&self.config);
        let config = Arc::make_mut(&mut new_config);
        config.add_backend(msg.group_idx, msg.ci);
        if let Some(removed_backend) = msg.replaced {
            config.remove_backend(&removed_backend);
        }
        self.config = new_config.clone();
        let path = self.config_path.clone();
        Box::pin(async move { save_config(path, new_config).await })
    }
}

impl Handler<ReplaceMetaBackend> for ConfigActor {
    type Result = ResponseFuture<Result<(), ZstorError>>;

    fn handle(&mut self, msg: ReplaceMetaBackend, _: &mut Self::Context) -> Self::Result {
        let mut new_config = Arc::clone(&self.config);
        let config = Arc::make_mut(&mut new_config);
        let mut meta = config.meta().clone();
        match meta {
            Meta::Zdb(ref mut zdb_meta) => {
                match msg.new_nodes.try_into() {
                    Ok(nn) => {
                        zdb_meta.set_backends(nn);
                    }
                    Err(e) => {
                        // Should not happen
                        error!(
                            "Could't convert meta backend slice (len {}) to array of len 4",
                            e.len()
                        );
                    }
                };
            }
        }
        config.set_meta(meta);
        self.config = new_config.clone();
        let path = self.config_path.clone();
        Box::pin(async move { save_config(path, new_config).await })
    }
}

/// Save a config to the config file.
async fn save_config(config_path: PathBuf, config: Arc<Config>) -> Result<(), ZstorError> {
    let data = toml::to_vec(config.deref()).map_err(ZstorError::from)?;
    Ok(fs::write(&config_path, data)
        .await
        .map_err(|e| ZstorError::new_io("Could not save config file".into(), e))?)
}
