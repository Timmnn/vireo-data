//Combines multiple files from the data folder into one

import fs from "fs";
import path from "path";

const input_folder = "./data/";
const output_file = "combined_output.csv";
const deduped_output_file = "deduped." + output_file;

const isFile = (fileName: string) => {
  return fs.lstatSync(fileName).isFile();
};

const files = fs
  .readdirSync(input_folder)
  .map((fileName) => {
    return path.join(input_folder, fileName);
  })
  .filter(isFile)
  .filter((file_path) => !file_path.includes(".broken"));

for (const [i, file_path] of files.entries()) {
  const content = fs.readFileSync(file_path).toString();

  const lines = content.split("\n");
  lines.push("");

  fs.appendFileSync(output_file, lines.join("\n"));
}

const seen = new Set(); // To track seen lines
const uniqueLines = [] as string[]; // To store unique lines

// Read the file line by line
fs.readFileSync(output_file, "utf-8")
  .split("\n") // Split the file content into lines
  .forEach((line) => {
    const trimmedLine = line.trim(); // Remove leading/trailing whitespace
    if (trimmedLine && !seen.has(trimmedLine)) {
      seen.add(trimmedLine);
      uniqueLines.push(trimmedLine);
    }
  });

// Write the unique lines to a new file
fs.writeFileSync(deduped_output_file, uniqueLines.join("\n"), "utf-8");
