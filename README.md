# Vireo Data

This repository is part of the Vireo Project and is not to usefull on its own.
Here you can find the specification of the data formats required for the Vireo Project.

## Directory Structure
The data has to be structured like the following filetree.

data/
├── equities/
│   └── [SYMBOL]/
│       ├── [DATASET]/
│       │   ├── data.parquet
│       │   └── symbol-meta.json
│       └── dataset-meta.json
└── futures/
    ├── [SYMBOL]/
    │   └── [CONTRACT]/
    │       └── [DATASET]/
    │           ├── data.parquet
    │           └── dataset-meta.json
    └── symbol-meta.json

## Datasets
### $PERIOD_OHLCV
#### Columns:
- open: ?f32
- high: ?f32
- low: ?f32
- close: ?f32
- volume?: u32
- datetime: DateTime (End of Period. E.g. if a 1-hour bar goes from 9am to 10am, this value has to represent 10am)

## Data Generator
This repository also includes a tool to generate fake data, to test things out.
This can be used by navigating to /fake-data-generator/ filling out the example.config.json and running "cargo run;"


## Metadata
WIP
