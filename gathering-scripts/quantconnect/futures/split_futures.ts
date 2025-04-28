import * as fs from "fs";
import * as path from "path";
import csv from "csv-parser"; // Correct import for csv-parser

interface CsvRow {
  expiry: string;
  symbol: string;
  time: string;
  askclose: string;
  askhigh: string;
  asklow: string;
  askopen: string;
  asksize: string;
  bidclose: string;
  bidhigh: string;
  bidlow: string;
  bidopen: string;
  bidsize: string;
  close: string;
  high: string;
  low: string;
  open: string;
  volume: string;
}

const inputFilePath = "../deduped.combined_output.csv"; // Replace with your input file path
const outputDir = "../x"; // Directory to store the output files

if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir);
}

const expiryMap: { [key: string]: CsvRow[] } = {};

fs.createReadStream(inputFilePath)
  .pipe(csv()) // Correct usage of csv-parser
  .on("data", (row: CsvRow) => {
    if (!expiryMap[row.expiry]) {
      expiryMap[row.expiry] = [];
    }
    expiryMap[row.expiry].push(row);
  })
  .on("end", () => {
    for (const expiry in expiryMap) {
      const safeExpiry = expiry.replace(/[: ]/g, "_"); // Sanitize expiry for filename
      console.log("DBG1", safeExpiry);
      const outputFilePath = path.join(outputDir, `${safeExpiry}.csv`);
      const rows = expiryMap[expiry];
      const header = Object.keys(rows[0]).join(",");
      const content = rows
        .map((row) => Object.values(row).join(","))
        .join("\n");

      console.log(`Writing file: ${outputFilePath}`);
      fs.writeFileSync(outputFilePath, `${header}\n${content}`);
    }
  });
