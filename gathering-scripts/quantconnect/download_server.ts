import { Hono } from "hono";
import { serve } from "@hono/node-server";
import * as fs from "fs/promises";
import * as path from "path";
import { bodyLimit } from "hono/body-limit";
import zlib from "zlib";
import { promisify } from "util";
import { transform } from "typescript";
const gunzip = promisify(zlib.gunzip);

const app = new Hono();

// Health check endpoint
app.get("/", (c) => {
  return c.json({
    status: "online",
    timestamp: new Date().toISOString(),
    message: "Server is running",
  });
});

// POST endpoint to handle data
app.post(
  "/data",
  bodyLimit({
    maxSize: 50 * 1024 * 1024 * 1024, // 50gb
    onError: (c) => {
      return c.text("overflow :(", 413);
    },
  }),

  async (c) => {
    try {
      if (!(await fs.exists("./data/"))) {
        await fs.mkdir("./data/");
      }

      let data = (await c.req.json()).data as string;

      const fileNumber = data.split("___")[0];
      data = data.split("___")[1];

      const fileName = path.join("data", `${fileNumber}.csv`);

      if (await fs.exists(fileName)) {
        return c.json({});
      }

      let is_error = false;
      let uncompressed = await gunzip(Buffer.from(data, "base64"))
        .then((x) => x)
        .catch((err) => {
          is_error = true;
          return "";
        });

      uncompressed = uncompressed
        .toString()
        .split("\n")
        .map((line) => line.split(",").slice(1).join(","))
        .join("\n");

      //TRANSFORMATION

      uncompressed = transformCsv(uncompressed, {}, []);

      console.log(uncompressed);

      if (is_error) {
        await fs.writeFile(fileName + ".broken", data);
      } else {
        await fs.writeFile(fileName, uncompressed);
      }

      return c.json({
        success: true,
        message: `Data stored in file ${fileNumber}.csv`,
        fileNumber,
      });
    } catch (error) {
      console.log(error);
      return c.json(
        {
          success: false,
          message: "Error storing data",
          error: error.message,
        },
        500,
      );
    }
  },
);

function transformCsv(
  data: string,
  transformer: {
    [key: string]: {
      new_name?: string;
      transformer?: (before: string) => string;
    };
  },

  drop_columns: string[],
) {
  const lines = data.split("\n");

  let header = lines.shift()!.split(",");

  let new_lines = [] as string[];

  // Apply transformations
  for (const line of lines) {
    let fields = line.split(",");

    for (let [key, modification] of Object.entries(transformer)) {
      if (!modification.transformer) continue;

      const index = header.indexOf(key);

      fields[index] = modification.transformer(fields[index]);
    }

    new_lines.push(fields.join(","));
  }

  //Rename Headers
  for (let [key, modification] of Object.entries(transformer)) {
    header[header.indexOf(key)] = modification.new_name;
  }

  //Drop Columns

  for (const column of drop_columns) {
    const index = header.indexOf(column);

    header = header.toSpliced(index, 1);

    for (let [i, line] of new_lines.entries()) {
      new_lines[i] = line.split(",").toSpliced(index, 1).join(",");
    }
  }

  const joined_header = header.join(",");

  return [joined_header, ...new_lines].join("\n");
}

Bun.serve({
  fetch: app.fetch,
  port: 5729, // Change this to the desired port
});
