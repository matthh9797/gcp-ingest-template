import yfinance as yf 
import pandas as pd


def download_single_history(ticker: str, period: str) -> pd.DataFrame:
    """
    Retrieve historical financial data for a single tickers from yfinance api
    @param tickers list of stock tickers
    @param period how far to go back (e.g. 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
    @return dict containing api output
    """
    yfobj = yf.Ticker(ticker)
    hist = yfobj.history(period=period)
    return hist


def download_stock_history(tickers: list, period: str = '5d') -> dict:
    """
    Retrieve historical financial data for multiple tickers from yfinance api
    @param tickers list of stock tickers
    @param period how far to go back (e.g. 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
    @return dict containing api output
    """
    for index, ticker in enumerate(tickers):
        hist = download_single_history(ticker, period)
        hist['ticker'] = ticker
        if index == 0:
            df = hist
        else:
            df = pd.concat([df, hist])
    return df


def transform_stock_history(hist_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transform for stock history dataframe
    @param hist_raw stock history directly from yfinance api
    @return transformed dataframe
    """
    return hist_raw.reset_index()