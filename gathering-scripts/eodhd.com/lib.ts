import assert from "assert";
import { log } from "console";
import { z } from "zod";

const exchangeSchema = z.array(
    z
        .object({
            Name: z.string(),
            Code: z.string(),
            OperatingMIC: z.string().or(z.null()),
            Country: z.string(),
            Currency: z.string(),
            CountryISO2: z.string(),
            CountryISO3: z.string(),
        })
        .transform((exchange) => {
            return {
                name: exchange.Name,
                code: exchange.Code,
                operatingMic: exchange.OperatingMIC, // or operatingMIC if you prefer camelCase
                country: exchange.Country,
                currency: exchange.Currency,
                countryIso2: exchange.CountryISO2, // or countryISO2
                countryIso3: exchange.CountryISO3, // or countryISO3
            };
        }),
);

type Exchange = z.infer<typeof exchangeSchema>[number];

const API_KEY = process.env.API_KEY;
assert.equal(!!API_KEY, true);

export const getAllExchanges = async () => {
    const res = await fetch(
        `https://eodhd.com/api/exchanges-list/?api_token=${API_KEY}&fmt=json`,
    ).then((res) => res.json());

    const data = exchangeSchema.parse(res);

    return data;
};

const tickersSchema = z.array(
    z
        .object({
            Name: z.string(),
            Code: z.string(),
            Country: z.string(),
            Exchange: z.string(),
            Currency: z.string(),
            Type: z.string(),
            Isin: z.string().or(z.null()),
        })
        .transform((ticker) => {
            return {
                name: ticker.Name,
                code: ticker.Code,
                country: ticker.Country,
                exchange: ticker.Exchange,
                currency: ticker.Currency,
                type: ticker.Type,
                isin: ticker.Isin,
            };
        }),
);

export const getAllTickers = async (exchange_code: Exchange["code"]) => {
    const res = await fetch(
        `https://eodhd.com/api/exchange-symbol-list/${exchange_code}?api_token=${API_KEY}&fmt=json`,
    ).then((res) => res.json());

    const data = tickersSchema.parse(res);

    return data;
};

const stockDataSchema = z.object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "Invalid date format"),
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    adjusted_close: z.number(),
    volume: z.number(),
});

const stockDataArraySchema = z.array(stockDataSchema);

export const getDataForTicker = async (
    ticker: string,
    exchange_code: string,
) => {
    const res = await fetch(
        `https://eodhd.com/api/eod/${ticker}.${exchange_code}?api_token=${API_KEY}&fmt=json`,
    ).then((res) => res.json());

    const data = stockDataArraySchema.parse(res);

    return data;
};
