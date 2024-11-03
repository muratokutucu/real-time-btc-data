# real-time-btc-data

Each minute, prints bitcoin current price, 24H-High, 24H-Low, 24H-Volume, and the SMA calculated with last 30 candles on 5m timeframe.

## Table of Contents

- [Libraries](#libraries)
- [Installation](#installation)
- [Usage](#usage)

## Libraries

For this project, I used:
- [ccxt](https://github.com/ccxt/ccxt) : in order to easily retrieve data from exchange (KuCoin)
- [pandas](https://github.com/pandas-dev/pandas) : for data manipulation/transformation purpose
  

## Installation

```
pip install -r requirements.txt
```

## Usage

```
python real_time_btc_data.py
```
