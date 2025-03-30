mod config_reader;
mod generators;
mod parquet_writer;
use config_reader::read_config;
use generators::Generator;
use parquet_writer::write_parquet;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() {
    let contents = fs::read_to_string("example.config.json").unwrap();

    let json: serde_json::Value =
        serde_json::from_str(&contents).expect("JSON was not well-formatted");

    let config = read_config("example.config.json").unwrap();

    for dataset in config.datasets {
        fs::create_dir_all(format!("./data/{}", &dataset.name)).unwrap();

        match dataset.dataset_type.to_lowercase().as_str() {
            "equities" => {
                let mut df = crate::generators::equities::EquitiesGenerator::generate(&dataset);

                write_parquet(
                    &mut df,
                    format!(
                        "{}{}{}{}",
                        config.data_path, "equities/", dataset.name, "/data.parquet"
                    ),
                );

                write_meta_file(format!(
                    "{}{}{}{}",
                    config.data_path, "equities/", dataset.name, "/meta.json"
                ));
            }
            "futures" => todo!(),
            _ => panic!("Invalid Type"),
        }

        ///////////
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MetaFile {
    pub is_fake: bool,
}

fn write_meta_file(file_path: String) -> () {
    let mut file = std::fs::File::create(file_path).unwrap();

    let mut writer = BufWriter::new(file);

    serde_json::to_writer(&mut writer, &MetaFile { is_fake: true });
    writer.flush();
}
