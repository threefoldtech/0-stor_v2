use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    zdb_index_dir_path: PathBuf,
    zdb_data_dir_path: PathBuf,
    zstor_config_path: PathBuf,
    zstor_bin_path: PathBuf,
    vdc_config: Option<VdcConfig>,
    /// Maximum size of the data dir in MiB, if this is set and the sum of the file sizes in the
    /// data dir gets higher than this value, the least used, already encoded file will be removed.
    max_zdb_data_dir_size: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VdcConfig {
    url: String,
    password: String,
    /// Size to request for new backends, in GB
    new_size: usize,
}

impl Config {
    pub fn zdb_index_dir_path(&self) -> &PathBuf {
        &self.zdb_index_dir_path
    }

    pub fn zdb_data_dir_path(&self) -> &PathBuf {
        &self.zdb_data_dir_path
    }

    pub fn zstor_config_path(&self) -> &PathBuf {
        &self.zstor_config_path
    }

    pub fn zstor_bin_path(&self) -> &PathBuf {
        &self.zstor_bin_path
    }

    pub fn vdc_config(&self) -> &Option<VdcConfig> {
        &self.vdc_config
    }

    pub fn max_zdb_data_dir_size(&self) -> Option<usize> {
        self.max_zdb_data_dir_size
    }
}

impl VdcConfig {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn new_size(&self) -> usize {
        self.new_size
    }
}
