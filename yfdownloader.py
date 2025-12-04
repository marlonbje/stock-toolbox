import yfinance as yf
import pandas as pd
from pathlib import Path
import logging

def get_ohlc(ticker=None,interval='1d'):
    folder = Path('yfdata')
    df = pd.DataFrame()
    if isinstance(ticker,(list,tuple)):
        return df
    try:
        if Path(ticker).exists():
            return df
    except Exception as e:
        pass
    if not folder.exists() or not folder.is_dir():
        folder.mkdir()
        
    path = Path(f'{folder}/{ticker}_ohlc_{interval}.csv')
    
    if not path.exists() or not path.is_file():
        if ticker is None:
            logging.info('Please enter a ticker symbol.')
            return df
        try:
            logging.info(f'Downloading data for {ticker}...')
            df = yf.download(ticker,interval=interval,period='10y',auto_adjust=True)
            df.columns = ['Open','High','Low','Close','Volume']
            df.to_csv(path)
            logging.info(f'Data for {ticker} saved to {folder}.\n')
        except Exception as e:
            logging.error(f'Error downloading {ticker}\n{e}\n.')
            return df
    else:
        logging.info(f'{path} found in {folder}.')
        try:
            df = pd.read_csv(path,index_col=0,parse_dates=True)
        except TypeError:
            logging.error(f'{path} is not a csv.\n')
            return df
    return df
    

def get_fundamentals(stock=None,freq='quarterly'):
    folder = Path('yfdata')
    df = pd.DataFrame()
    if isinstance(stock,(list,tuple)):
        return df
    try:
        if Path(stock).exists():
            return df
    except Exception as e:
        pass
    if not folder.exists() or not folder.is_dir():
        folder.mkdir()
    
    path = Path(f'{folder}/{stock}_fundamentals_{freq}.csv')
    
    if not path.exists() or not path.is_file():
        if stock is None:
            logging.info('Please enter a ticker symbol.')
            return df 
        try:
            logging.info(f'Downlaoding data for {stock}...')
            ticker = yf.Ticker(stock)
            
            bs = ticker.get_balancesheet(freq=freq)
            cf = ticker.get_cashflow(freq=freq)
            inc = ticker.get_incomestmt(freq=freq)
            
            df = pd.concat([bs,cf,inc],axis=0,join='inner').T
            if freq == 'quarterly':
                df.index = df.index.to_period('Q')
            else:
                df.index = df.index.year
                
            df.sort_index(inplace=True)
            df = df.T
            
            df.to_csv(path)
            logging.info(f'Data for {stock} saved to {folder}.\n')
        except Exception as e:
            logging.error(f'Error downloading {stock}\n {e}.\n')
            return df
    else:
        logging.info(f'{path} data found in {folder}.')
        try:
            df = pd.read_csv(path,index_col=0,parse_dates=True)
        except TypeError:
            logging.error(f'{path} is not a csv.\n')
            return df
    return df
            
    