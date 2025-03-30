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
