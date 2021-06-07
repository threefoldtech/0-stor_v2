use crate::{config::Config, ZstorError};
use actix::prelude::*;
use std::{path::PathBuf, sync::Arc};
use tokio::fs;

#[derive(Message)]
#[rtype(result = "Arc<Config>")]
/// Message to request the current config
pub struct GetConfig;

#[derive(Message)]
#[rtype(result = "Result<(), ZstorError>")]
/// Message to trigger a config reload
pub struct ReloadConfig;

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
