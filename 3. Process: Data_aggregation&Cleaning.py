import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)
import numpy as np
# API key from third party data source
from config import api_key
from time import sleep

# Set the path to the files
p = Path('C:\\Users\\ferna\\Python\\2008_2020_US_Small_Cap\\2019_2020_data')

# Find the files; this is a generator, not a list
files = p.glob('ticker_*.csv')

# Concatenate all csv files
df = pd.concat([pd.read_csv(file) for file in files])
df = pd.read_csv('tickers_2019_2022.csv')

# Close column has already been adjusted
df.drop(columns='Adj Close',inplace=True)

# Transform columns names to lower case
df.columns = df.columns.str.lower()

# Get the previous closing price
df['prev_close'] = df.groupby('ticker')['close'].shift()

# Drop tickers without prev_close value
df=df.dropna()

# Create gap up and intraday gain percentage column 
df['gap_up'] = (df.open-df.prev_close)/df.prev_close * 100
df['intraday_gain'] = (df.high - df.open)/df.open * 100
df['sum_gap_intraday']  = df.gap_up + df.intraday_gain

# Convert volume column to interger
df.volume.astype('int')

# Filter out tickers that went up over 300% including gap up with minimum vol at 5M ensuring liquity issue
df_filtered = df.loc[(df.sum_gap_intraday>300)&(df.volume>5000000)]

# Reset the index starts from 0 and sort the date 
df_filtered = df_filtered.sort_values('date').reset_index(drop=True)

# Swap ticker column to the first positon
first_column = df_filtered.pop('ticker')
df_filtered.insert(0, 'ticker', first_column)

# Grab exchange&shares_outstanding data from a reliable third party source
q = 1
x = 0
# X will loop through the ticker and date position, every loop will have +1 increment change
while q == 1:
    ticker = df.iloc[x,0]
    date = df.iloc[x,1]
    # Check whether last two columns is NaN value or not
    if pd.isna(df.iloc[x,-1]) or pd.isna(df.iloc[x,-2]) == True:
        try:
            stockinfo=f'https://api.polygon.io/v3/reference/tickers/{ticker}?date={date}&apiKey={api_key}'
            data = requests.get(stockinfo).json()
            result = data['results']
        except KeyError:
            print('no data found, ready for next one','index: ',x)
            # if data is not found, X still need to plus 1 in order to jump into next ticker 
            x+=1
            sleep(20)
            # Continue will ignore all the remaining codes and start over
            continue
        # Fill in exchange column
        if data['results'].get('primary_exchange') == 'XNYS':
            df['exchange'].iloc[x] = 'nyse'
        elif data['results'].get('primary_exchange') == 'XNAS':
            df['exchange'].iloc[x] = 'nasdaq'
        elif data['results'].get('primary_exchange') == 'XASE':
            df['exchange'].iloc[x] = 'Amex'
        else:
            df['exchange'].iloc[x] = 'others'
        # Get shares outstanding data    
        if data['results'].get('share_class_shares_outstanding') == None:
            df['shares_outstanding'].iloc[x] = data['results'].get('weighted_shares_outstanding')
        else:
            df['shares_outstanding'].iloc[x] = data['results'].get('share_class_shares_outstanding')    
    else:
        print('not nan value','index: ', x)
        pass
    x+=1
    # Download data every 20 sec due to 5 API call per minute limitation
    sleep(20)
    # Total of 92 rows(Python index starts with 0), the loop will be broke once it hits 93
    if x == 93:
        break
        
# Add sector column
df['sector'] = ''

# Loop through tickers and download sector data
x=0
q=1
while q == 1:
    ticker = df.iloc[x,0]
    df.iloc[x,-1] = si.get_company_info(ticker).iloc[1,0]
    print('index: ',x,' completed')
    x+=1
    if x >92:
        print('out of index!')
        break
        
# Calculate market cap and dollar volume
df['market_cap'] = round(df.shares_outstanding * df.prev_close,0).astype('int64')
df.insert(6, 'dollar_volume', round(df.close*df.volume,0).astype('int64'))

# Transform date to datetime and create dayofweek column
df.date = pd.to_datetime(df.date)
df['dayofweek'] = df.dayofweek.replace({'Monday':'Mon','Tuesday':'Tue','Wednesday':'Wed','Thursday':'Thur','Friday':'Fri'})

# Last step: Data Validation
def validation(dic,df):
    # Validate data type: Iterate through dictionary
    for key,col in dic.items():
        if key == 'string':
            # for every column names under string
            for col in df[col]:
                if df[col].dtype != np.object_:
                    print('not object column:', df[col].name,', data type:',df[col].dtype)
        elif key == 'interger':
            for col in df[col]:
                if df[col].dtype != np.int64:
                #if np.issubdtype(df[col].dtype, np.int) != True:
                    print('not int64 column:', df[col].name,', data type:',df[col].dtype)
        elif key == 'float':
            for col in df[col]:
                if df[col].dtype != np.float64:
                    print('not float64 column:', df[col].name,', data type:',df[col].dtype)
        elif key == 'datetime':
            if df[col].dtype != 'datetime64[ns]':
                print('not datetime64 column:', df[col].name,', data type:',df[col].dtype)
        else:
            print('column not found')
    # Validate duplicated values
    print('-----------------------------------------')
    print('duplicated values: ',df.duplicated().sum())
    print('-----------------------------------------')
    # Validate NaN values
    print('check NaN values:','\n',df.isna().sum())
    
# Define a dictionary with designed columns and data types
dtype_validation = {'string':['ticker','exchange', 'sector','dayofweek'],
                    'interger':['dollar_volume', 'volume', 'shares_outstanding','market_cap'],
                     'float':['open','high','low','close','prev_close','gap_up','intraday_gain','sum_gap_intraday'],
                     'datetime':'date'}
# Validate
validation(dtype_validation,df)
