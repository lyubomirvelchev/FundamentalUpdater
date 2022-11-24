EOD_API_KEY = '60941c62f10668.99813942'
MAIN_URL = 'http://eodhistoricaldata.com/api/bulk-fundamentals/'
SYMBOL_LIST_URL = 'https://eodhistoricaldata.com/api/exchange-symbol-list/US?api_token='
INDICES_URL = 'https://api.twelvedata.com/indices?country=United%20States'
FOREX_URL = 'https://api.twelvedata.com/forex_pairs'
MAIN_TABLE_NAME = 'General'
SPECIAL_TABLES = ['Earnings', 'Financials']
TICKER_TYPES = ['ETF', 'Common Stock', 'Preferred Stock']
EXCHANGE_LIST_V1 = ['NASDAQ']
EXCHANGE_LIST_V2 = ['NYSE', 'NYSE ARCA', 'NYSE MKT']
EXCHANGE_LIST_V3 = ['PINK']
EXCHANGE_LIST_V4 = ['BATS', 'OTCQX', 'OTCQB', 'OTCCE', 'OTCGREY', 'OTCMKTS', 'OTCBB', 'AMEX', 'NMFQS']
MAX_NUMBER_OF_TICKERS_PER_QUERY = 999
MAX_NUMBER_OF_TICKERS_IN_URL = 500

MAIN_TABLE_COLUMNS = ['asset_id', 'Code', 'Type', 'Name',
                      'Exchange', 'CurrencyCode', 'CurrencyName',
                      'CurrencySymbol',
                      'CountryName', 'CountryISO', 'ISIN', 'CUSIP',
                      'Sector', 'Industry', 'Description',
                      'FullTimeEmployees', 'UpdatedAt']

HIGHLIGHTS_TABLE_COLUMNS = ['MarketCapitalization', 'MarketCapitalizationMln', 'EBITDA', 'PERatio', 'PEGRatio',
                            'WallStreetTargetPrice', 'BookValue', 'DividendShare', 'DividendYield', 'EarningsShare',
                            'EPSEstimateCurrentYear', 'EPSEstimateNextYear', 'EPSEstimateNextQuarter',
                            'MostRecentQuarter', 'ProfitMargin', 'OperatingMarginTTM', 'ReturnOnAssetsTTM',
                            'ReturnOnEquityTTM', 'RevenueTTM', 'RevenuePerShareTTM', 'QuarterlyRevenueGrowthYOY',
                            'GrossProfitTTM', 'DilutedEpsTTM', 'QuarterlyEarningsGrowthYOY'] + ['TickerID']

VALUATION_TABLE_COLUMNS = ['TrailingPE', 'ForwardPE', 'PriceSalesTTM', 'PriceBookMRQ', 'EnterpriseValueRevenue',
                           'EnterpriseValueEbitda'] + ['TickerID']

TECHNICALS_TABLE_COLUMNS = ['Beta', '52WeekHigh', '52WeekLow', '50DayMA', '200DayMA', 'SharesShort',
                            'SharesShortPriorMonth', 'ShortRatio', 'ShortPercent'] + ['TickerID']

SPLITS_DIVIDENDS_TABLE_COLUMNS = ['ForwardAnnualDividendRate', 'ForwardAnnualDividendYield', 'PayoutRatio',
                                  'DividendDate', 'ExDividendDate', 'LastSplitFactor', 'LastSplitDate'] + ['TickerID']

EARNINGS_TABLE_COLUMNS = ['date', 'epsActual', 'epsEstimate', 'epsDifference', 'surprisePercent'] + ['TickerID']

FINANCIALS_TABLE_COLUMNS = ['currency_symbol', 'quarterly_last_0', 'quarterly_last_1', 'quarterly_last_2',
                            'quarterly_last_3', 'yearly_last_0', 'yearly_last_1', 'yearly_last_2', 'yearly_last_3'] + [
                               'TickerID']
NON_INCLUDE_COLUMNS = ['LEI']

STRUCTURE = {
    'General': {col: [] for col in MAIN_TABLE_COLUMNS},
    'Highlights': {col: [] for col in HIGHLIGHTS_TABLE_COLUMNS},
    'Valuation': {col: [] for col in VALUATION_TABLE_COLUMNS},
    'Technicals': {col: [] for col in TECHNICALS_TABLE_COLUMNS},
    'SplitsDividends': {col: [] for col in SPLITS_DIVIDENDS_TABLE_COLUMNS},
    'Earnings': {
        'Last_0': {col: [] for col in EARNINGS_TABLE_COLUMNS},
        'Last_1': {col: [] for col in EARNINGS_TABLE_COLUMNS},
        'Last_2': {col: [] for col in EARNINGS_TABLE_COLUMNS},
        'Last_3': {col: [] for col in EARNINGS_TABLE_COLUMNS},
    },
    'Financials': {
        'Balance_Sheet': {col: [] for col in FINANCIALS_TABLE_COLUMNS},
        'Cash_Flow': {col: [] for col in FINANCIALS_TABLE_COLUMNS},
        'Income_Statement': {col: [] for col in FINANCIALS_TABLE_COLUMNS},
    }
}

CRYPTO_TABLE_COLUMNS = ['Id', 'Url', 'ImageUrl', 'ContentCreatedOn', 'Name', 'Symbol', 'CoinName', 'FullName',
                        'Description', 'AssetTokenStatus', 'Algorithm', 'ProofType', 'SortOrder', 'Sponsored',
                        'Taxonomy_Access', 'Taxonomy_FCA', 'Taxonomy_FINMA', 'Taxonomy_Industry',
                        'Taxonomy_CollateralizedAsset', 'Taxonomy_CollateralizedAssetType', 'Taxonomy_CollateralType',
                        'Taxonomy_CollateralInfo', 'Weiss_Rating', 'Weiss_TechnologyAdoptionRating',
                        'Weiss_MarketPerformanceRating', 'IsTrading', 'TotalCoinsMined', 'CirculatingSupply',
                        'BlockNumber', 'NetHashesPerSecond', 'BlockReward', 'BlockTime', 'AssetLaunchDate',
                        'AssetWhitepaperUrl', 'AssetWebsiteUrl', 'MaxSupply', 'MktCapPenalty', 'IsUsedInDefi',
                        'IsUsedInNft', 'PlatformType', 'AlgorithmType', 'Difficulty', 'BuiltOn', 'SmartContractAddress',
                        'DecimalPoints']

CRYPTO_STRUCTURE_DICT = {col: [] for col in CRYPTO_TABLE_COLUMNS}

FOREX_TABLE_COLUMNS = ['asset_id', 'symbol', 'name']
FOREX_PAIRS_TABLE_COLUMNS = ['asset_id', 'symbol', 'currency_group', 'currency_base', 'currency_quote']
INDICES_TABLE_COLUMNS = ['asset_id', 'symbol', 'name', 'country', 'currency']

FOREX_TABLE_STRUCTURE = {col: [] for col in FOREX_TABLE_COLUMNS}
FOREX_PAIRS_TABLE_STRUCTURE = {col: [] for col in FOREX_PAIRS_TABLE_COLUMNS}
INDICES_TABLE_STRUCTURE = {col: [] for col in INDICES_TABLE_COLUMNS}

CRYPTO_SPECIAL_COLUMNS = ['Taxonomy', 'Rating', 'Symbol']
CRYPTO_URL = 'https://min-api.cryptocompare.com/data/all/coinlist'
