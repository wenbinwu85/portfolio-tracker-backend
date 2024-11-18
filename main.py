import warnings
import json
from typing import Dict
from datetime import datetime
from yahooquery import Ticker
from quart import Quart, jsonify, request
from quart_cors import cors

warnings.filterwarnings("ignore")

app = Quart(__name__)
app = cors(app)

# ALPACA_API_KEY = 'PK3OKOYQHL2RVL2DZ2UC'
# ALPACA_SECRET_KEY = 'I5LdfklL2g6shWULe4XMkXdeMlUQ7cNvHQBJBwTD'
# alpaca_api_base_url = 'https://data.alpaca.markets/v1beta1'

"""
--------- Yahooquery ----------
"""

YQ_MODULES = [
    'assetProfile',  # Information related to the company's location, operations, and officers.
    'calendarEvents',  # Earnings and Revenue expectations for upcoming earnings date, ex-dividend date, dividend date
    'defaultKeyStatistics',  # KPIs for given symbol(s) (PE, enterprise value, EPS, EBITA, and more
    'earnings',  # Historical earnings data
    'earningsHistory',  # Data related to historical earnings (actual vs. estimate)
    'earningsTrend',  # Historical trend data for earnings and revenue estimations
    'financialData',  # Financial KPIs
    'fundOwnership',  # Top 10 owners of a given symbol(s)
    'fundPerformance',  # Historical return data for a given symbol(s) and symbol(s) specific category
    'fundProfile',  # Summary level information for a given symbol(s)
    'indexTrend',  # Trend data related given symbol(s) index, specificially PE and PEG ratios
    'insiderHolders',  # Data related to stock holdings of a given symbol(s) insiders
    'insiderTransactions',  # Transactions by insiders
    'institutionOwnership',  # Top 10 owners of a given symbol(s)
    'majorHoldersBreakdown',  # Data showing breakdown of owners of given symbol(s), insiders, institutions, etc.
    'price',  # Detailed pricing data for given symbol(s), exchange, quote type, currency, market cap, pre / post market data, etc.
    'recommendationTrend',  # Data related to historical recommendations (buy, hold, sell)
    'summaryDetail',  # Contains information available via the Summary tab
    'topHoldings',
    'upgradeDowngradeHistory'
]

BASE_MODULES = {
    "asset_profile": "Asset Profile",
    "calendar_events": "Calendar Events",
    "esg_scores": "ESG Scores",
    "financial_data": "Financial Data",
    "fund_profile": "Fund Profile",
    "key_stats": "Key Statistics",
    "major_holders": "Major Holders",
    "price": "Pricing",
    "quote_type": "Quote Type",
    "share_purchase_activity": "Share Purchase Activity",
    "summary_detail": "Summary Detail",
    "summary_profile": "Summary Profile",
    "balance_sheet": "Balance Sheet",
    "cash_flow": "Cash Flow",
    "company_officers": "Company Officers",
    "earning_history": "Earning History",
    "earnings": "Earnings",
    "earnings_trend": "Earnings Trend",
    "index_trend": "Index Trend",
    "sector_trend": "Sector Trend",
    "industry_trend": "Industry Trend",
    "fund_ownership": "Fund Ownership",
    "grading_history": "Grading History",
    "income_statement": "Income Statement",
    "insider_holders": "Insider Holders",
    "insider_transactions": "Insider Transactions",
    "institution_ownership": "Institution Ownership",
    "recommendation_trend": "Recommendation Trends",
    "sec_filings": "SEC Filings",
    "fund_bond_holdings": "Fund Bond Holdings",
    "fund_bond_ratings": "Fund Bond Ratings",
    "fund_equity_holdings": "Fund Equity Holdings",
    "fund_holding_info": "Fund Holding Information",
    "fund_performance": "Fund Performance",
    "fund_sector_weightings": "Fund Sector Weightings",
    "fund_top_holdings": "Fund Top Holdings",
}


def init_ticker(symbols, **kwargs):
    return Ticker(symbols, **kwargs)


def get_module_data(ticker: Ticker, attribute: str, *args, **kwargs) -> Dict:
    try:
        return getattr(ticker, attribute)(*args, **kwargs)
    except TypeError:
        return getattr(ticker, attribute)


def map_modules_data(yq_modules_data):
    mapped_data = {}
    for symbol, ticker_data in yq_modules_data.items():
        mapped_symbol_data = {}
        mapped_symbol_data.update(ticker_data['price'])
        mapped_symbol_data.update(ticker_data['summaryDetail'])
        mapped_symbol_data.update(ticker_data['defaultKeyStatistics'])
        mapped_symbol_data['profile'] = ticker_data['assetProfile']

        quoteType = ticker_data['price']['quoteType']
        ticker_price = ticker_data['price']['regularMarketPrice']['raw']

        if quoteType == 'EQUITY':
            mapped_symbol_data.update(ticker_data['financialData'])
            mapped_symbol_data['calendarEvents'] = ticker_data['calendarEvents']
            mapped_symbol_data['earnings'] = ticker_data['earnings']
            mapped_symbol_data['earnings'].update(ticker_data['earningsHistory'])
            mapped_symbol_data['earnings'].update(ticker_data['earningsTrend'])
            mapped_symbol_data['indexTrend'] = ticker_data['indexTrend']
            mapped_symbol_data['insiderTransactions'] = ticker_data.get('insiderTransactions', {}).get('transactions', {})
            mapped_symbol_data['recommendationTrend'] = ticker_data['recommendationTrend']['trend']
            mapped_symbol_data['shareholders'] = {}
            mapped_symbol_data['shareholders']['fundOwnership'] = ticker_data['fundOwnership']['ownershipList']
            mapped_symbol_data['shareholders']['insiderHolders'] = ticker_data['insiderHolders']['holders']
            mapped_symbol_data['shareholders']['institutionOwnership'] = ticker_data['institutionOwnership']['ownershipList']
            mapped_symbol_data['shareholders']['majorHolders'] = ticker_data.get('majorHoldersBreakdown', {})
            mapped_symbol_data['upgradeDowngradeHistory'] = ticker_data['upgradeDowngradeHistory']['history'][:10]

            try:
                free_cashflow = mapped_symbol_data['freeCashflow']['raw']
                free_cashflow_per_share = free_cashflow / mapped_symbol_data['sharesOutstanding']['raw']
                mapped_symbol_data['freeCashflowPerShare'] = free_cashflow_per_share
                mapped_symbol_data['freeCashflowYield'] = round(free_cashflow_per_share / ticker_price, 4)
                if mapped_symbol_data['freeCashflowPerShare'] != 0:
                    mapped_symbol_data['freeCashflowPayoutRatio'] = mapped_symbol_data['dividendRate']['raw'] / free_cashflow_per_share
                else:
                    mapped_symbol_data['freeCashflowPayoutRatio'] = 0
                mapped_symbol_data['enterpriseValueToFreeCashflow'] = round(mapped_symbol_data['enterpriseValue']['raw'] / free_cashflow, 2)
            except KeyError:
                mapped_symbol_data['freeCashflowPerShare'] = 0
                mapped_symbol_data['freeCashflowYield'] = 0
                mapped_symbol_data['freeCashflowPayoutRatio'] = 0
                mapped_symbol_data['enterpriseValueToFreeCashflow'] = 0
        else:
            mapped_symbol_data['profile'].update(ticker_data['fundProfile'])
            mapped_symbol_data['dividendRate'] = ticker_data['summaryDetail']['yield']['raw'] * ticker_price
            mapped_symbol_data['dividendYield'] = ticker_data['summaryDetail']['yield']['raw']
            mapped_symbol_data['topHoldings'] = ticker_data['topHoldings']
            mapped_symbol_data['fundPerformance'] = ticker_data['fundPerformance']
        
        clean_up_mapped_symbol_data(mapped_symbol_data)
        mapped_data[symbol] = mapped_symbol_data
    return mapped_data


def clean_up_mapped_symbol_data(mapped_symbol_data):
    keys = [
        'algorithm', 'ask', 'askSize', 'bid', 'bidSize', 
        'category', 'circulatingSupply', 'coinMarketCapLink', 'expireDate', 'fromCurrency', 
        'legalType', 'maxAge', 'priceHint', 'startDate', 'strikePrice',
        'toCurrency', 'tradeable', 'underlyingSymbol'
    ]
    for k, v in mapped_symbol_data.items():
        if (isinstance(v, (dict, str)) and not len(v)):
            keys.append(k)
        if (v is None):
            keys.append(k)
    for key in set(keys):
        try:
            del mapped_symbol_data[key]
        except:
            print(f'failed to delete key {key}')
    del mapped_symbol_data['profile']['companyOfficers']


def fetch_stock_data(symbols):
    ticker = init_ticker(symbols, formatted=True, asynchronous=True)
    try:
        modules_data = get_module_data(ticker, 'get_modules', modules=YQ_MODULES)
        return map_modules_data(modules_data)
    except Exception as e:
        print(f'failed to fetch modules data for {symbols}:', e)
        return None


def yq_dividend_history(symbol, start_date):
    ticker = init_ticker(symbol, formatted=True, asynchronous=True)
    try:
        return get_module_data(ticker, 'dividend_history', start=start_date)
    except Exception as e:
        print(symbol, 'failed to fetch dividend history:', e)
        return None


def yq_technical_insights(symbol):
    ticker = init_ticker(symbol, formatted=True, asynchronous=True)
    try:
        return get_module_data(ticker, 'technical_insights')
    except Exception as e:
        print(symbol, 'failed to fetch technical insights:', e)
        return None


def yq_corporate_events(symbol):
    ticker = init_ticker(symbol, formatted=True, asynchronous=True)
    try:
        return get_module_data(ticker, 'corporate_events')
    except Exception as e:
        print(symbol, 'failed to fetch corporate events:', e)
        return None


"""
--------- Quart ----------
"""


@app.route('/')
def index():
    return '<h1>Shoo! Nothing to see here. Go away!</h1>'


@app.route('/fetch/stock/<symbol>')
def get_stock_data(symbol):
    data = fetch_stock_data(symbol)
    if data is not None:
        return data
    else:
        return f'<h1>Unable to fetch data for symbol: {symbol} </h1>'


@app.route('/fetch/portfolio/<symbols>')
def fetch_stocks_data(symbols):
    symbols = symbols.split(':')
    data = fetch_stock_data(symbols)
    if data is not None:
        return data
    else:
        return f'<h1>Unable to fetch data for symbols: {symbols} </h1>'


@app.route('/fetch/technical-insights/<symbol>')
def fetch_technical_insights(symbol):
    data = yq_technical_insights(symbol)
    return jsonify(data)


@app.route('/fetch/portfolio/technical-insights/<symbols>')
def fetch_portfolio_technical_insights(symbols):
    symbols = symbols.split(':')
    data = yq_technical_insights(symbols)
    return jsonify(data)


@app.route('/fetch/dividend-history/<symbol>')
def fetch_dividend_history(symbol):
    div_his = {}
    thisYear = datetime.now().year
    year = int(request.args.get('years', 10))
    start_date = str(thisYear - year) + '-01-01'
    div_his_data = yq_dividend_history(symbol, start_date)
    for line in div_his_data.to_csv().split()[1:]:
        _, div_date, div_rate = line.split(',')
        div_his[div_date] = div_rate
    return jsonify(div_his)


@app.route('/fetch/events/<symbol>')
def fetch_corporate_events(symbol):
    data = yq_corporate_events(symbol)
    data = data.to_dict(orient='split')
    timestamps = data['index']
    articles = data['data']
    events = {symbol: []}
    for article in zip(timestamps, articles):
        timestamp = article[0][1]
        if timestamp.year == datetime.now().year:
            event = {
                'symbol': article[0][0],
                'time': timestamp.value,
                'displayTime': f'{timestamp.day_name()}, {timestamp.month_name()} {timestamp.day} {timestamp.year}',
                'title': article[1][2],
                'text': article[1][3]
            }
            events[symbol].append(event)
    return jsonify(events)


if __name__ == '__main__':
    # symbol = 'nke'
    # data = fetch_stock_data(symbol)
    # print(data)
    # with open("ticker_data.json", "w") as file:
    #     json.dump(data, file)
    # data = yq_technical_insights(symbol)
    # with open("ticker_tech_insights_data.json", "w") as file:
    #     json.dump(data, file)
    # data = yq_dividend_history(symbol, start_date='05-20-2020') # returns pandas.DataFrame
    # with open("ticker_dividends_data.json", "w") as file:
    #     json.dump(data, file)

    # data = yq_corporate_events(symbol) # returns pandas.DataFrame
    # print(data)

    # symbols = ['T', 'vz']
    # data = fetch_stock_data(symbols)
    # data = yq_dividend_history(symbols, start_date='05-01-2020') # returns pandas.DataFrame
    # data_json = data.to_json()
    # data = yq_technical_insights(symbols)
    # data = yq_corporate_events(symbols) # returns pandas.DataFrame
    # print(data)
    
    # div_his = {}
    # year = int(request.args.get('year', datetime.now().year))
    # start_date = str(2024) + '-01-01'
    # div_his_data = yq_dividend_history('DVN', start_date)
    # for line in div_his_data.to_csv().split()[1:]:
    #     _, div_date, div_rate = line.split(',')
    #     div_his[div_date] = div_rate
    # print(div_his)
    
    app.run(debug=True)
