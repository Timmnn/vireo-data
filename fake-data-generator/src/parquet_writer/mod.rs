use std::{fs, path::Path};

use polars::prelude::*;

pub fn write_parquet(df: &mut DataFrame, file_path: String) -> () {
    let file_path = Path::new(&file_path);

    let directory = file_path.parent().unwrap();

    fs::create_dir_all(directory);

    println!("Writing File: {:?}", fs::canonicalize(&file_path));
    let mut file = std::fs::File::create(file_path).unwrap();

    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Brotli(None)) // Use None for default Brotli level
        .finish(df)
        .unwrap();
}

