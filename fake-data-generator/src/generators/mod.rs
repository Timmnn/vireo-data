pub mod equities;
use polars::frame::DataFrame;

use crate::config_reader::DatasetConfig;

pub trait Generator {
    fn generate(config: &DatasetConfig) -> DataFrame;
}
