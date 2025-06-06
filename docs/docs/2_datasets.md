# Datasets

## \{PERIOD\}\_OHLCV

#### Columns:

- open: ?f32
- high: ?f32
- low: ?f32
- close: ?f32
- volume: ?u32
- adjusted_close: ?f32
- datetime: u64 (Represents the end timestamp in ms of the interval. For example, a 1-hour candle from 9:00 to 10:00 will have datetime set to 10:00. This marks the moment the data becomes valid and complete. With 1d or above, the value is not in ms anymore but in days where 1970-1-1 equals 0. On 1d, the value represents the day the data is from, with 1w or 1mo, the day represents the last day of the period)

## Metadata

### symbol-meta.json

```json
{
  "name": "NAME OF THE STOCK/FUTURES/...",
  "ticker": "OFFICIAL code for identification.of the symbol",
  "country": "The country, the symbol is traded in",
  "exchange": "The code of the exchange",
  "isin": "https://en.wikipedia.org/wiki/International_Securities_Identification_Number",
  "download_date": "The timestamp, the data was downloaded",
  "source": "Where the data came from",
  "type": "more fine symbol category. For example fund for equities or commodity for futures"
}
```

## Data Generator

This repository will also include a tool to generate fake data, to test things out.
This can be used by navigating to /fake-data-generator/ filling out the example.config.json and running "cargo run"
