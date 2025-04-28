import time
import gzip
import base64
from datetime import datetime
from IPython.display import display, clear_output
import time

qb = QuantBook()


def compress_string(input_string):
    compressed_data = gzip.compress(input_string.encode("utf-8"))
    compressed_string = base64.b64encode(compressed_data).decode("utf-8")
    return compressed_string


def decompress_string(compressed_string):
    compressed_data = base64.b64decode(compressed_string.encode("utf-8"))
    decompressed_data = gzip.decompress(compressed_data)
    return decompressed_data.decode("utf-8")


def filter_function(fundamentals):
    sorted_by_pe_ratio = sorted(
        [f for f in fundamentals],
        key=lambda fundamental: fundamental.valuation_ratios.pe_ratio,
    )
    return [fundamental.symbol for fundamental in sorted_by_pe_ratio]


futures = [
    Futures.Currencies.AUD,
    Futures.Currencies.AUDCAD,
    Futures.Currencies.AUDJPY,
    Futures.Currencies.AUDNZD,
    Futures.Currencies.BRL,
    Futures.Currencies.BTC,
    Futures.Currencies.CAD,
    Futures.Currencies.CADJPY,
    Futures.Currencies.CHF,
    Futures.Currencies.ETH,
    Futures.Currencies.EUR,
    Futures.Currencies.EURAUD,
    Futures.Currencies.EURCAD,
    Futures.Currencies.EURO_FX_EMINI,
    Futures.Currencies.EURSEK,
    Futures.Currencies.GBP,
    Futures.Currencies.JAPANESE_YEN_EMINI,
    Futures.Currencies.JPY,
    Futures.Currencies.MICRO_AUD,
    Futures.Currencies.MICRO_BTC,
    Futures.Currencies.MICRO_CAD,
    Futures.Currencies.MICRO_CADUSD,
    Futures.Currencies.MICRO_CHF,
    Futures.Currencies.MICRO_ETHER,
    Futures.Currencies.MICRO_EUR,
    Futures.Currencies.MICRO_GBP,
    Futures.Currencies.MICRO_INRUSD,
    Futures.Currencies.MICRO_JPY,
    Futures.Currencies.MICRO_USDCHF,
    Futures.Currencies.MICRO_USDCNH,
    Futures.Currencies.MICRO_USDJPY,
    Futures.Currencies.MXN,
    Futures.Currencies.NZD,
    Futures.Currencies.RUB,
    Futures.Currencies.STANDARD_SIZE_USD_OFFSHORE_RMBCNH,
    Futures.Currencies.ZAR,
    Futures.Dairy.CASH_SETTLED_BUTTER,
    Futures.Dairy.CASH_SETTLED_CHEESE,
    Futures.Dairy.CLASS_III_MILK,
    Futures.Dairy.CLASS_IV_MILK,
    Futures.Dairy.DRY_WHEY,
    Futures.Dairy.NONFAT_DRY_MILK,
    Futures.Energy.ARGUS_LL_SVS_WTI_ARGUS_TRADE_MONTH,
    Futures.Energy.ARGUS_PROPANE_FAR_EAST_INDEX,
    Futures.Energy.ARGUS_PROPANE_SAUDI_ARAMCO,
    Futures.Energy.BRENT_CRUDE_OIL_VS_DUBAI_CRUDE_OIL_PLATTS,
    Futures.Energy.BRENT_LAST_DAY_FINANCIAL,
    Futures.Energy.CHICAGO_ETHANOL_PLATTS,
    Futures.Energy.CLEARBROOK_BAKKEN_SWEET_CRUDE_OIL_MONTHLY_INDEX_NET_ENERGY,
    Futures.Energy.CONWAY_PROPANE_OPIS,
    Futures.Energy.CRUDE_OIL_WTI,
    Futures.Energy.DUBAI_CRUDE_OIL_PLATTS_FINANCIAL,
    Futures.Energy.EAST_WEST_GASOLINE_SPREAD_PLATTS_ARGUS,
    Futures.Energy.EAST_WEST_NAPHTHA_JAPAN_C_FVS_CARGOES_CIFNWE_SPREAD_PLATTS,
    Futures.Energy.ETHANOL,
    Futures.Energy.ETHANOL_T_2_FOB_RDAM_INCLUDING_DUTY_PLATTS,
    Futures.Energy.EUROPEAN_NAPHTHA_PLATTS_CRACK_SPREAD,
    Futures.Energy.EUROPEAN_PROPANE_CIFARA_ARGUS,
    Futures.Energy.EUROPEAN_PROPANE_CIFARA_ARGUS_VS_NAPHTHA_CARGOES_CIFNWE_PLATTS,
    Futures.Energy.FREIGHT_ROUTE_TC_14_BALTIC,
    Futures.Energy.GASOLINE,
    Futures.Energy.GASOLINE_EUROBOB_OXY_NWE_BARGES_ARGUS,
    Futures.Energy.GROUP_THREE_SUBOCTANE_GASOLINE_PLATTS_VS_RBOB,
    Futures.Energy.GROUP_THREE_ULSD_PLATTS_VS_NY_HARBOR_ULSD,
    Futures.Energy.GULF_COAST_CBOB_GASOLINE_A_2_PLATTS_VS_RBOB_GASOLINE,
    Futures.Energy.GULF_COAST_HSFO_PLATTS_VS_EUROPEAN_THREE_POINT_FIVE_PERCENT_FUEL_OIL_BARGES_FOB_RDAM_PLATTS,
    Futures.Energy.HEATING_OIL,
    Futures.Energy.LOS_ANGELES_CARBOB_GASOLINE_OPI_SVS_RBOB_GASOLINE,
    Futures.Energy.LOS_ANGELES_CARB_DIESEL_OPI_SVS_NY_HARBOR_ULSD,
    Futures.Energy.LOS_ANGELES_JET_OPI_SVS_NY_HARBOR_ULSD,
    Futures.Energy.MARS_ARGUS_VS_WTI_FINANCIAL,
    Futures.Energy.MARS_ARGUS_VS_WTI_TRADE_MONTH,
    Futures.Energy.MICRO_CRUDE_OIL_WTI,
    Futures.Energy.MICRO_EUROPEAN_FOB_RDAM_MARINE_FUEL_ZERO_POINT_FIVE_PERCENT_BARGES_PLATTS,
    Futures.Energy.MICRO_EUROPEAN_THREE_POINT_FIVE_PERCENT_OIL_BARGES_FOB_RDAM_PLATTS,
    Futures.Energy.MICRO_GASOIL_ZERO_POINT_ONE_PERCENT_BARGES_FOBARA_PLATTS,
    Futures.Energy.MICRO_SINGAPORE_FOB_MARINE_FUEL_ZERO_POINT_FIVE_PERCET_PLATTS,
    Futures.Energy.MICRO_SINGAPORE_FUEL_OIL_380_CST_PLATTS,
    Futures.Energy.MINI_EUROPEAN_THREE_POINT_PERCENT_FIVE_FUEL_OIL_BARGES_PLATTS,
    Futures.Energy.MINI_SINGAPORE_FUEL_OIL_180_CST_PLATTS,
    Futures.Energy.MONT_BELVIEU_ETHANE_OPIS,
    Futures.Energy.MONT_BELVIEU_LDH_PROPANE_OPIS,
    Futures.Energy.MONT_BELVIEU_NATURAL_GASOLINE_OPIS,
    Futures.Energy.MONT_BELVIEU_NORMAL_BUTANE_OPIS,
    Futures.Energy.NATURAL_GAS,
    Futures.Energy.NATURAL_GAS_HENRY_HUB_LAST_DAY_FINANCIAL,
    Futures.Energy.NATURAL_GAS_HENRY_HUB_PENULTIMATE_FINANCIAL,
    Futures.Energy.ONE_PERCENT_FUEL_OIL_CARGOES_FOBNWE_PLATTS_VS_THREE_POINT_FIVE_PERCENT_FUEL_OIL_BARGES_FOB_RDAM_PLATTS,
    Futures.Energy.PREMIUM_UNLEADED_GASOLINE_10_PPM_FOBMED_PLATTS,
    Futures.Energy.PROPANE_NON_LDH_MONT_BELVIEU_OPIS,
    Futures.Energy.RBOB_GASOLINE_CRACK_SPREAD,
    Futures.Energy.RBOB_GASOLINE_VS_EUROBOB_OXY_NWE_BARGES_ARGUS_THREE_HUNDRED_FIFTY_THOUSAND_GALLONS,
    Futures.Energy.SINGAPORE_FUEL_OIL_380_CST_PLATTS_VS_EUROPEAN_THREE_POINT_FIVE_PERCENT_FUEL_OIL_BARGES_FOB_RDAM_PLATTS,
    Futures.Energy.SINGAPORE_GASOIL_PLATTS_VS_LOW_SULPHUR_GASOIL_FUTURES,
    Futures.Energy.SINGAPORE_MOGAS_92_UNLEADED_PLATTS_BRENT_CRACK_SPREAD,
    Futures.Energy.THREE_POINT_FIVE_PERCENT_FUEL_OIL_BARGES_FOB_RDAM_PLATTS_CRACK_SPREAD,
    Futures.Energy.THREE_POINT_FIVE_PERCENT_FUEL_OIL_BARGES_FOB_RDAM_PLATTS_CRACK_SPREAD_1000_MT,
    Futures.Energy.WTI_BRENT_FINANCIAL,
    Futures.Energy.WTI_FINANCIAL,
    Futures.Energy.WTI_HOUSTON_ARGUS_VS_WTI_TRADE_MONTH,
    Futures.Energy.WTI_HOUSTON_CRUDE_OIL,
    Futures.Financials.EURO_DOLLAR,
    Futures.Financials.FIVE_YEAR_USDMAC_SWAP,
    Futures.Financials.MICRO_Y_10_TREASURY_NOTE,
    Futures.Financials.MICRO_Y_2_TREASURY_BOND,
    Futures.Financials.MICRO_Y_30_TREASURY_BOND,
    Futures.Financials.MICRO_Y_5_TREASURY_BOND,
    Futures.Financials.ULTRA_TEN_YEAR_US_TREASURY_NOTE,
    Futures.Financials.ULTRA_US_TREASURY_BOND,
    Futures.Financials.Y_10_TREASURY_NOTE,
    Futures.Financials.Y_2_TREASURY_NOTE,
    Futures.Financials.Y_30_TREASURY_BOND,
    Futures.Financials.Y_5_TREASURY_NOTE,
    Futures.Forestry.LUMBER,
    Futures.Forestry.RANDOM_LENGTH_LUMBER,
    Futures.Grains.BLACK_SEA_CORN_FINANCIALLY_SETTLED_PLATTS,
    Futures.Grains.BLACK_SEA_WHEAT_FINANCIALLY_SETTLED_PLATTS,
    Futures.Grains.CORN,
    Futures.Grains.HRW_WHEAT,
    Futures.Grains.OATS,
    Futures.Grains.SOYBEANS,
    Futures.Grains.SOYBEAN_MEAL,
    Futures.Grains.SOYBEAN_OIL,
    Futures.Grains.WHEAT,
    Futures.Indices.BLOOMBERG_COMMODITY_INDEX,
    Futures.Indices.DOW_30_E_MINI,
    Futures.Indices.DOW_JONES_REAL_ESTATE,
    Futures.Indices.FTSE_EMERGING_EMINI,
    Futures.Indices.MICRO_DOW_30_E_MINI,
    Futures.Indices.MICRO_NASDAQ_100_E_MINI,
    Futures.Indices.MICRO_RUSSELL_2000_E_MINI,
    Futures.Indices.MICRO_SP_500_E_MINI,
    Futures.Indices.NASDAQ_100_BIOTECHNOLOGY_E_MINI,
    Futures.Indices.NASDAQ_100_E_MINI,
    Futures.Indices.NIKKEI_225_DOLLAR,
    Futures.Indices.NIKKEI_225_YEN_CME,
    Futures.Indices.RUSSELL_1000_E_MINI,
    Futures.Indices.RUSSELL_2000_E_MINI,
    Futures.Indices.SPGSCI_COMMODITY,
    Futures.Indices.SP_400_MID_CAP_EMINI,
    Futures.Indices.SP_500_ANNUAL_DIVIDEND_INDEX,
    Futures.Indices.SP_500_E_MINI,
    Futures.Indices.TOPIXYEN,
    Futures.Indices.USD_DENOMINATED_IBOVESPA,
    Futures.Indices.VIX,
    Futures.Meats.FEEDER_CATTLE,
    Futures.Meats.LEAN_HOGS,
    Futures.Meats.LIVE_CATTLE,
    Futures.Metals.ALUMINIUM_EUROPEAN_PREMIUM_DUTY_PAID_METAL_BULLETIN,
    Futures.Metals.ALUMINUM_MWUS_TRANSACTION_PREMIUM_PLATTS_25_MT,
    Futures.Metals.COPPER,
    Futures.Metals.GOLD,
    Futures.Metals.MICRO_GOLD,
    Futures.Metals.MICRO_GOLD_TAS,
    Futures.Metals.MICRO_PALLADIUM,
    Futures.Metals.MICRO_SILVER,
    Futures.Metals.PALLADIUM,
    Futures.Metals.PLATINUM,
    Futures.Metals.SILVER,
    Futures.Metals.US_MIDWEST_DOMESTIC_HOT_ROLLED_COIL_STEEL_CRU_INDEX,
    Futures.Softs.SUGAR_11,
    Futures.Softs.SUGAR_11_CME,
]


for future in futures:
    pass


symbol = qb.add_future(
    Futures.Indices.SP_500_E_MINI,
    Resolution.Hour,
    data_normalization_mode=DataNormalizationMode.BACKWARDS_RATIO,
    data_mapping_mode=DataMappingMode.LAST_TRADING_DAY,
    contract_depth_offset=0,
)

symbol.set_filter(-10000000, 10000000)

start_date = datetime(1970, 1, 1)
end_date = datetime(2025, 5, 1)

all_contracts = []

# Iterate through each date in the range
current_date = start_date
while current_date <= end_date:
    contracts = qb.future_chain_provider.get_future_contract_list(
        symbol.symbol, current_date
    )
    all_contracts.extend(contracts)
    current_date += timedelta(days=10)  # Move to the next day

count = 0
lines = 0

for i, symbol in enumerate(all_contracts):
    count += 1
    df = qb.History(symbol, 1000000, Resolution.Hour)
    df.reset_index(inplace=True)

    step = 6000
    for j in range(0, len(df.index), step):
        chunk = df.iloc[i : i + step]
        csv = chunk.to_csv()
        # display(len(csv.splitlines()))
        lines += len(csv.splitlines())
        compressed_chunk = compress_string(csv)
        display("XXXXXXXXXX" + str(count) + "___" + compressed_chunk + "XXXXXXXXXX")
        time.sleep(5)
        clear_output()
