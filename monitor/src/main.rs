use log::LevelFilter;
use log::{debug, error, info, trace, warn};
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Config as LogConfig, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::{Filter, Response};
use structopt::StructOpt;

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
        default_value = "zstor.log",
        long,
        parse(from_os_str)
    )]
    log_file: std::path::PathBuf,
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

fn main() {
    let args = Args::from_args();

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
}
