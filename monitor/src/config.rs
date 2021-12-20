use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    zdb_data_dir_path: PathBuf,
    zstor_config_path: PathBuf,
    zstor_bin_path: PathBuf,
    /// Maximum size of the data dir in MiB, if this is set and the sum of the file sizes in the
    /// data dir gets higher than this value, the least used, already encoded file will be removed.
    max_zdb_data_dir_size: Option<usize>,
    #[serde(default = "default_ns_filled_treshold")]
    zdb_namespace_fill_treshold: u8,
    vdc_config: Option<VdcConfig>,
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

    pub fn zdb_namespace_fill_treshold(&self) -> u8 {
        self.zdb_namespace_fill_treshold
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

fn default_ns_filled_treshold() -> u8 {
    95
}

#[cfg(test)]
mod tests {
    use super::{Config, VdcConfig};

    #[test]
    fn test_serialize_config() {
        let config = Config {
            zdb_data_dir_path: "/tmp/zdb/zdb-index".parse().unwrap(),
            zstor_config_path: "/tmp/zstor_config.toml".parse().unwrap(),
            zstor_bin_path: "/tmp/zstor_v2".parse().unwrap(),
            max_zdb_data_dir_size: Some(1024 * 50), // 50 GiB
            zdb_namespace_fill_treshold: 90,        // 90% before rotation
            vdc_config: Some(VdcConfig {
                url: "https://some.evdc.tech".to_string(),
                password: "supersecurepassword".to_string(),
                new_size: 20, // 20 GB
            }),
        };

        let output = toml::to_string(&config).unwrap();

        // let expected = r#""zdb_data_dir_path = \"/tmp/zdb/zdb-index\"\nzstor_config_path = \"/tmp/zstor_config.toml\"\nzstor_bin_path = \"/tmp/zstor_v2\"\nmax_zdb_data_dir_size = 51200\nzdb_namespace_fill_treshold = 90\n\n[vdc_config]\nurl = \"https://some.evdc.tech\"\npassword = \"supersecurepassword\"\nnew_size = 20\n"#;
        let expected = r#"zdb_data_dir_path = "/tmp/zdb/zdb-index"
zstor_config_path = "/tmp/zstor_config.toml"
zstor_bin_path = "/tmp/zstor_v2"
max_zdb_data_dir_size = 51200
zdb_namespace_fill_treshold = 90

[vdc_config]
url = "https://some.evdc.tech"
password = "supersecurepassword"
new_size = 20
"#;

        println!("{}", output);

        assert_eq!(&output, expected);
    }

    #[test]
    fn test_deserialize_config() {
        let input = r#"
zdb_data_dir_path = "/tmp/zdb/zdb-index"
zstor_config_path = "/tmp/zstor_config.toml"
zstor_bin_path = "/tmp/zstor_v2"
max_zdb_data_dir_size = 51200
zdb_namespace_fill_treshold = 90

[vdc_config]
url = "https://some.evdc.tech"
password = "supersecurepassword"
new_size = 20
"#;

        let config = toml::from_str(input).unwrap();

        let output = Config {
            zdb_data_dir_path: "/tmp/zdb/zdb-index".parse().unwrap(),
            zstor_config_path: "/tmp/zstor_config.toml".parse().unwrap(),
            zstor_bin_path: "/tmp/zstor_v2".parse().unwrap(),
            max_zdb_data_dir_size: Some(1024 * 50), // 50 GiB
            zdb_namespace_fill_treshold: 90,        // 90% before rotation
            vdc_config: Some(VdcConfig {
                url: "https://some.evdc.tech".to_string(),
                password: "supersecurepassword".to_string(),
                new_size: 20, // 20 GB
            }),
        };

        assert_eq!(output, config);
    }
}
