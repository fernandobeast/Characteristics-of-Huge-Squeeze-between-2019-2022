import yfinance as yf
from yahoo_fin import stock_info as si
from pathlib import Path

# Get symbols from exchanges
nasdaq = si.tickers_nasdaq()
other = si.tickers_other()

# Convert list to set so no duplicated values will be excluded
nasdaq_set = set(nasdaq)
other_set = set(other)

# Merge two sets
symbols = set.union(nasdaq_set, other_set)

# Slice out any symbols more than 4 and below 2 characters
sav_set = set()
del_set = set()
for i in symbols:
    if len(i) <= 4 and len(i) >=2:
        sav_set.add(i)
    else:
        del_set.add(i)
print( f'Removed {len( del_set )} unqualified stock symbols...' )
print( f'There are {len( sav_set )} qualified stock symbols...' )

# Download datasets and store in a path
sav_lst = list(sav_set)
for ticker in sav_lst:
    try:
        data = yf.download(i, start="2019-01-01", end="2022-09-02", group_by="Ticker",threads = True,auto_adjust = True)
        data['Ticker'] = ticker
        data.to_csv(f'my_path\\ticker_{i}.csv')
    except (Exception, Error) as error:
        print(error)
