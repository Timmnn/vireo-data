import assert from "assert";
import moment from "moment";
import fs from "fs";
import { getAllExchanges, getAllTickers, getDataForTicker } from "./lib";
import { log } from "console";
import { parquetWriteFile } from "hyparquet-writer";
import path from "path";
import { exit } from "process";

function stringToUint8Array(str: string) {
    const encoder = new TextEncoder();
    return encoder.encode(str);
}

// 1st of Jan 1970 => 0. 10th of January 1970 => 9
function dateStringtoDaysInt(str: string) {
    const null_date = moment(0);
    const current = moment(str, "YYYY-MM-DD");

    return current.diff(null_date, "days");
}

const DATA_PATH = "./data/";

const GET_N_TICKERS = 10000;

const main = async () => {
    const exchanges = await getAllExchanges();

    let tickers_left_to_get = GET_N_TICKERS;
    for (const exchange of exchanges) {
        const tickers = await getAllTickers(exchange.code);

        for (const ticker of tickers) {
            if (tickers_left_to_get === 0) {
                exit(0);
            }

            console.log(
                `(${GET_N_TICKERS - tickers_left_to_get}/${GET_N_TICKERS}) Pulling: ${exchange.code} ${ticker.code}`,
            );

            const parquet_dir = path.join(
                DATA_PATH,
                exchange.code,
                ticker.code,
                "1d_OHLCV",
            );

            fs.mkdirSync(parquet_dir, { recursive: true });

            const parquet_path = path.join(parquet_dir, "data.parquet");

            if (fs.existsSync(parquet_path)) {
                console.log(
                    `${ticker.code}.${exchange.code} already exists... Skipping`,
                );
                continue;
            }

            console.log(ticker);

            const data = await getDataForTicker(ticker.code, exchange.code);

            fs.writeFileSync(
                path.join(parquet_dir, "meta.json"),
                JSON.stringify(
                    {
                        source: "eodhc.com",
                        download_date: new Date().toISOString(),
                        name: ticker.name,
                        ticker: ticker.code,
                        country: ticker.country,
                        exchange: ticker.exchange,
                        type: ticker.type,
                        isin: ticker.isin,
                    },
                    null,
                    2,
                ),
            );

            tickers_left_to_get--;

            parquetWriteFile({
                filename: parquet_path,
                columnData: [
                    {
                        name: "date",
                        data: data
                            .map((row) => row.date)
                            .map(dateStringtoDaysInt),
                        type: "INT32",
                    },
                    {
                        name: "open",
                        data: data.map((row) => row.open),
                        type: "FLOAT",
                    },
                    {
                        name: "high",
                        data: data.map((row) => row.high),
                        type: "FLOAT",
                    },
                    {
                        name: "low",
                        data: data.map((row) => row.low),
                        type: "FLOAT",
                    },
                    {
                        name: "close",
                        data: data.map((row) => row.close),
                        type: "FLOAT",
                    },

                    {
                        name: "adjusted_close",
                        data: data.map((row) => row.adjusted_close),
                        type: "FLOAT",
                    },

                    {
                        name: "volume",
                        data: data.map((row) => row.volume).map(BigInt),
                        type: "INT64",
                    },
                ],
            });
        }
    }
};

main();
