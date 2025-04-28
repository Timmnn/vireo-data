# Directory Structure

The data has to be structured like the following filetree.

- data
  - equities
    - [EXCHANGE] # NYSE, NASDAQ, NSE, ...
      - [SYMBOL] # AAPL, MSFT, MMM, ...
        - [DATASET] # 1h_OHLCV, 1m_OHLCV, 1M_OHLCV, ...
          - data.parquet
          - symbol-meta.json
        - dataset-meta.json
  - futures
    - [EXCHANGE]
      - [SYMBOL]
        - [CONTRACT]
          - [DATASET]
            - data.parquet
            - dataset-meta.json
    - symbol-meta.json
