use log::LevelFilter;
use log::{info, trace};
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Config as LogConfig, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::{Filter, Response};
use std::path::PathBuf;
use structopt::StructOpt;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::runtime::Builder;
use tokio::signal;
use zstor_monitor::config::Config;
use zstor_monitor::{ErrorKind as MonitorErrorKind, Monitor, MonitorError, MonitorResult};

#[derive(StructOpt, Debug)]
#[structopt(about = "zstor monitor")]
/// Zstor monitor
///
/// This executable monitors the health of remote 0-db backends, and has the option to do cleanup
/// of local data files. It is created to operate as part of the quantum storage solution.
struct Args {
    /// Path to the log file to use. The logfile will automatically roll over if the size
    /// increases beyond 10MiB.
    #[structopt(
        name = "log_file",
        default_value = "monitor.log",
        long,
        parse(from_os_str)
    )]
    log_file: std::path::PathBuf,
    #[structopt(
        name = "config",
        default_value = "monitor_config.toml",
        short,
        long,
        parse(from_os_str)
    )]
    config: std::path::PathBuf,
}

const MIB: u64 = 1 << 20;

/// ModuleFilter is a naive log filter which only allows (child modules of) a given module.
#[derive(Debug)]
struct ModuleFilter {
    module: String,
}

impl Filter for ModuleFilter {
    fn filter(&self, record: &log::Record) -> Response {
        if let Some(mod_path) = record.module_path() {
            // this is technically not correct but sufficient for our purposes
            if mod_path.starts_with(self.module.as_str()) {
                return Response::Neutral;
            }
        }
        Response::Reject
    }
}

fn main() -> MonitorResult<()> {
    let args = Args::from_args();

    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        // Realistically this should never happen. If it does happen, its fatal anyway.
        .expect("Could not build configure program runtime");

    rt.block_on(async {
        // TODO: add check for file name
        let mut rolled_log_file = args.log_file.clone();
        let name = if let Some(ext) = rolled_log_file.extension() {
            format!(
                "{}.{{}}.{}",
                rolled_log_file.file_stem().unwrap().to_str().unwrap(),
                ext.to_str().unwrap(),
            )
        } else {
            format!(
                "{}.{{}}",
                rolled_log_file.file_stem().unwrap().to_str().unwrap(),
            )
        };
        rolled_log_file.set_file_name(name);

        // init logger
        let policy = CompoundPolicy::new(
            Box::new(SizeTrigger::new(10 * MIB)),
            Box::new(
                FixedWindowRoller::builder()
                    .build(rolled_log_file.to_str().unwrap(), 5)
                    .unwrap(),
            ),
        );
        let log_file = RollingFileAppender::builder()
            .append(true)
            .encoder(Box::new(PatternEncoder::new(
                "{d(%Y-%m-%d %H:%M:%S %Z)(local)}: {l} {m}{n}",
            )))
            .build(&args.log_file, Box::new(policy))
            .unwrap();
        let log_config = LogConfig::builder()
            .appender(
                Appender::builder()
                    .filter(Box::new(ModuleFilter {
                        module: "zstor_v2".to_string(),
                    }))
                    .build("logfile", Box::new(log_file)),
            )
            .logger(Logger::builder().build("filelogger", LevelFilter::Debug))
            .build(
                Root::builder()
                    .appender("logfile")
                    .build(log::LevelFilter::Debug),
            )
            .unwrap();
        log4rs::init_config(log_config).unwrap();

        let config = load_config(&args.config).await?;

        let monitor = Monitor::new(config);
        let (ctx, handle) = monitor.start().await?;

        // spawn a task to wait for ctrl-c
        tokio::spawn(async {
            signal::ctrl_c()
                .await
                .expect("failed to listen for SIGINT events");
            info!("SIGINT received, shutting down");
            drop(ctx);
        });

        Ok(handle.await?)
    })
}

pub async fn load_config(path: &PathBuf) -> MonitorResult<Config> {
    trace!("reading config");
    let mut cfg_file = File::open(path)
        .await
        .map_err(|e| MonitorError::new_io(MonitorErrorKind::Config, e))?;
    let mut cfg_str = String::new();
    cfg_file
        .read_to_string(&mut cfg_str)
        .await
        .map_err(|e| MonitorError::new_io(MonitorErrorKind::Config, e))?;

    let cfg: Config = toml::from_str(&cfg_str)?;
    Ok(cfg)
}
