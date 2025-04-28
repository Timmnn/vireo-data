import * as parquet from "parquetjs";
import path from "path";
import fs from "fs";
import moment from "moment";
import { parse } from "csv-parse/sync";

// 1st of Jan 1970 => 0. 10th of January 1970 => 9
function dateStringtoDaysInt(str: string) {
  if (!str || str.trim() === "") return null;
  try {
    const null_date = moment(0);
    const current = moment(str, "YYYY-MM-DD");
    if (!current.isValid()) return null;
    return current.diff(null_date, "days");
  } catch (e) {
    console.warn(`Invalid date: ${str}`);
    return null;
  }
}

function datetimeStringtoUnix(str: string) {
  if (!str || str.trim() === "") return null;
  try {
    const timestamp = new Date(str).getTime();
    return isNaN(timestamp) ? null : timestamp;
  } catch (e) {
    console.warn(`Invalid datetime: ${str}`);
    return null;
  }
}

function safeParseFloat(str: string) {
  if (!str || str.trim() === "") return null;
  const num = parseFloat(str);
  return isNaN(num) ? null : num;
}

function safeParseInt(str: string) {
  if (!str || str.trim() === "") return null;
  const num = parseInt(str, 10);
  return isNaN(num) ? null : num;
}

// Define schema for our data
function createParquetSchema() {
  return new parquet.ParquetSchema({
    expiry: { type: "INT32", optional: true },
    symbol: { type: "UTF8" },
    time: { type: "INT64", optional: true },
    askclose: { type: "FLOAT", optional: true },
    askhigh: { type: "FLOAT", optional: true },
    asklow: { type: "FLOAT", optional: true },
    askopen: { type: "FLOAT", optional: true },
    asksize: { type: "FLOAT", optional: true },
    bidclose: { type: "FLOAT", optional: true },
    bidhigh: { type: "FLOAT", optional: true },
    bidlow: { type: "FLOAT", optional: true },
    bidopen: { type: "FLOAT", optional: true },
    bidsize: { type: "FLOAT", optional: true },
    close: { type: "FLOAT", optional: true },
    high: { type: "FLOAT", optional: true },
    low: { type: "FLOAT", optional: true },
    open: { type: "FLOAT", optional: true },
    volume: { type: "INT32", optional: true },
  });
}

async function processFile(file: string, inputPath: string) {
  const csvPath = path.join(inputPath, file);
  const parquetPath = csvPath.replace(/\.csv$/i, ".parquet");

  try {
    console.log(`Processing: ${csvPath}`);
    const csvData = fs.readFileSync(csvPath, "utf8");

    const data = parse(csvData, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    console.log(`Successfully parsed ${data.length} rows from ${csvPath}`);

    // Create schema
    const schema = createParquetSchema();

    // Create a new ParquetWriter
    const writer = await parquet.ParquetWriter.openFile(
      schema,
      parquetPath,
      {},
    );

    // Process and write each row
    for (const row of data) {
      try {
        const record = {
          expiry: dateStringtoDaysInt(row.expiry),
          symbol: row.symbol || "",
          time: datetimeStringtoUnix(row.time),
          askclose: safeParseFloat(row.askclose),
          askhigh: safeParseFloat(row.askhigh),
          asklow: safeParseFloat(row.asklow),
          askopen: safeParseFloat(row.askopen),
          asksize: safeParseFloat(row.asksize),
          bidclose: safeParseFloat(row.bidclose),
          bidhigh: safeParseFloat(row.bidhigh),
          bidlow: safeParseFloat(row.bidlow),
          bidopen: safeParseFloat(row.bidopen),
          bidsize: safeParseFloat(row.bidsize),
          close: safeParseFloat(row.close),
          high: safeParseFloat(row.high),
          low: safeParseFloat(row.low),
          open: safeParseFloat(row.open),
          volume: safeParseInt(row.volume),
        };

        await writer.appendRow(record);
      } catch (err) {
        console.error(`Error processing row:`, row, err);
      }
    }

    // Close the writer
    await writer.close();
    console.log(`Successfully converted ${csvPath} to ${parquetPath}`);
  } catch (error) {
    console.error(`Error processing file ${file}:`, error);
  }
}

async function main() {
  const INPUT_PATH = "../../data/futures/ES/1m_OHLCV/";
  const files = fs
    .readdirSync(INPUT_PATH)
    .filter((file) => file.toLowerCase().endsWith(".csv"));

  console.log(`Found ${files.length} CSV files to process`);

  // Process each file sequentially
  for (const file of files) {
    await processFile(file, INPUT_PATH);
  }
}

// Run the main function
main().catch((err) => {
  console.error("Unexpected error:", err);
  process.exit(1);
});
