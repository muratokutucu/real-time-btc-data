from abc import abstractmethod, ABC
from datetime import datetime

import ccxt
import pandas as pd
from ccxt import kucoin, Exchange


class ExchangeHelper:
    """
    Permits to fetch candles on exchange easily in a easy-to-use format (DataFrame).
    """

    @staticmethod
    def get_spot_dataframe(exchange: Exchange, spot_symbol: str, timeframe: str, number_of_candles: int, delete_last_candle: bool) -> pd.DataFrame:
        """
        Returns a dataframe containing the last available candles on exchange for a defined pair/timeframe.
        :param exchange: Exchange
        :param spot_symbol: The pair. Relative to the exchange
        :param timeframe: Timeframe ('1m', '5m', '15m', '30m', 1h', '2h', '4h', '8h', '12h', '1d', '3d', '1w'). Relative to the exchange
        :param number_of_candles: Number of candles
        :param delete_last_candle: Delete last fetched candle. Useful if the exchange always returns as last candle a not finished one
        :return: DataFrame containing candles data: 'open_time', 'open', 'high', 'low', 'close', 'volume'
        """
        candles: list = []
        while len(candles) == 0:
            try:
                # sometimes exchange returns an empty list or throw an exception,
                # that's why it's in a for loop and a try-catch for retry purpose
                candles = exchange.fetch_ohlcv(symbol=spot_symbol, timeframe=timeframe, limit=number_of_candles)
            except Exception:
                pass

        return ExchangeHelper.__build_dataframe(candles=candles, delete_last_candle=delete_last_candle)

    @staticmethod
    def __build_dataframe(candles: list, delete_last_candle: bool) -> pd.DataFrame:
        """
        Convert candles
        :param candles: the data. Also called 'k-lines' by exchanges.
        :param delete_last_candle: Delete last candle from result
        :return: a dataframe containing candles
        """
        df = pd.DataFrame(candles, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])

        # cast the timestamp into a human-readable format
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', errors='coerce')

        if delete_last_candle:
            # drop last row which is not a 'finished' candle (i.e time does not have reached its end time)
            df.drop(df.tail(1).index, inplace=True)

        return df


class SymbolTimeframe:
    """
    A class to represent a pair associated to a timeframe
    """

    def __init__(self, spot_symbol: str, timeframe: str, number_of_candle_per_update: int, must_delete_unfinished_candle: bool):
        self.__spot_symbol: str = spot_symbol
        self.__timeframe: str = timeframe
        self.__number_of_candle_per_update: int = number_of_candle_per_update
        self.__must_delete_unfinished_candle: bool = must_delete_unfinished_candle

    def get_spot_symbol(self) -> str:
        return self.__spot_symbol

    def get_timeframe(self) -> str:
        return self.__timeframe

    def get_number_of_candle_per_update(self) -> int:
        return self.__number_of_candle_per_update

    def must_delete_unfinished_candle(self) -> bool:
        return self.__must_delete_unfinished_candle

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SymbolTimeframe):
            return False
        return self.__spot_symbol == other.__spot_symbol and self.__timeframe == other.__timeframe

    def __hash__(self) -> int:
        return hash((self.__spot_symbol, self.__timeframe))


class NewCandleListener(ABC):
    """
    Contract that must be implemented by classes that want to subscribe to NewCandleNotifier
    """
    @abstractmethod
    def on_new_candle(self, symbol_timeframe: SymbolTimeframe) -> None:
        """
        Action that subscribers must realize when a new candle is detected.
        :param symbol_timeframe: pair/timeframe which has a new candle
        """
        pass


class NewCandleNotifier:
    """
    A class that check constantly for new candles for a defined group of pair/timeframe
    When a new candle is detected, it notifies objects that subscribed to this class.
    """

    def __init__(self, exchange: kucoin, symbol_timeframes: list[SymbolTimeframe]):
        self.__exchange: kucoin = exchange
        self.__symbol_timeframes: list[SymbolTimeframe] = symbol_timeframes
        self.__listeners: list = []
        self.__last_timestamp_per_symbol_timeframe: dict[SymbolTimeframe, int] = {}
        self.__must_run: bool = False

    def register_listener(self, listener: NewCandleListener) -> None:
        """
        Subscribe to this class in order to get notified when a new candle is available.
        """
        self.__listeners.append(listener)

    def start(self) -> None:
        """
        Starts looking for new candles on defined group of pair/timeframe.
        When a new candle is detected, it notifies subscribers.
        """
        self.__must_run = True
        self.__last_timestamp_per_symbol_timeframe = self.__get_initial_timestamps()
        while self.__must_run:
            try:
                for st in self.__symbol_timeframes:
                    old_timestamp: int = self.__last_timestamp_per_symbol_timeframe[st]
                    timestamp: int = self.__fetch_last_candle_timestamp_until_value(st)

                    if timestamp > old_timestamp:
                        self.__last_timestamp_per_symbol_timeframe[st] = timestamp
                        self.__notify_listeners(st)

            except Exception:
                pass

    def stop(self) -> None:
        """
        Stop class's activity (looking for new candles and notifying subscribers).
        """
        self.__must_run = False

    def __get_initial_timestamps(self) -> dict[SymbolTimeframe, int]:
        """
        Return the last candles' timestamps for a defined group of pair/timeframe
        :return: a dictionary associating the symbol/timeframe to the corresponding last candle timestamp
        """
        last_timestamp_per_symbol = {}
        for symbol in self.__symbol_timeframes:
            last_timestamp_per_symbol[symbol] = self.__fetch_last_candle_timestamp_until_value(symbol)

        return last_timestamp_per_symbol

    def __fetch_last_candle_timestamp_until_value(self, to_check: SymbolTimeframe) -> int:
        """
        Fetch last available candle timestamp for a defined pair/timeframe
        :param to_check: pair/timeframe to check for
        :return: last candle timestamp
        """
        candle = []
        while len(candle) == 0:
            symbol: str = to_check.get_spot_symbol()
            timeframe: str = to_check.get_timeframe()
            candle = self.__exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=1)

        return int(candle[0][0])

    def __notify_listeners(self, detected: SymbolTimeframe) -> None:
        """
        Notify subscribers that a new candle appeared for a specific pair/symbol
        :param detected: pair/timeframe
        """
        for listener in self.__listeners:
            listener.on_new_candle(detected)


class RealTimeBitcoinDataPrinter(NewCandleListener):
    """
    Print each minute the current BTC/USDT price, the 24h-High, the 24h-Low, the 24h-Volume,
    and the SMA calculated by using the last 30 candles on 5m timeframe (does not use current 5m candle as it is not finished yet).
    """

    def __init__(self):
        self.__btc_usdt_1d = SymbolTimeframe(spot_symbol='BTC/USDT', timeframe='1d', number_of_candle_per_update=1, must_delete_unfinished_candle=False)
        self.__btc_usdt_5m = SymbolTimeframe(spot_symbol='BTC/USDT', timeframe='5m', number_of_candle_per_update=30, must_delete_unfinished_candle=True)
        self.__btc_usdt_1m = SymbolTimeframe(spot_symbol='BTC/USDT', timeframe='1m', number_of_candle_per_update=1, must_delete_unfinished_candle=True)
        self.__data_per_symbol_timeframe: dict[SymbolTimeframe, pd.DataFrame] = {
            self.__btc_usdt_1d: pd.DataFrame(),
            self.__btc_usdt_5m: pd.DataFrame(),
            self.__btc_usdt_1m: pd.DataFrame()
        }
        self.__spot_exchange: kucoin = ccxt.kucoin()
        self.__notifier = NewCandleNotifier(exchange=self.__spot_exchange,
                                            symbol_timeframes=list(self.__data_per_symbol_timeframe.keys()))
        self.__notifier.register_listener(listener=self)
        self.__started: bool = False
        self.__refresh_count: int = 0
        self.__MAX_REFRESH: int = 5

        self.__init_data()
        self.__print_formatted_output()  # the first print is immediate and does not account as part of the 5 refresh

    def start(self):
        """
        Start print job.
        """
        if not self.__started:
            self.__started = True
            self.__init_data()
            self.__notifier.start()

    def on_new_candle(self, symbol_timeframe: SymbolTimeframe) -> None:
        """
        Each time a new candle appear for defined pair/timeframe, refresh corresponding data,
        transform data, and print it.
        :param symbol_timeframe:
        """
        self.__refresh_count += 1
        self.__update_data(symbol_timeframe=symbol_timeframe)

        if symbol_timeframe == self.__btc_usdt_1m:
            # each minute we must fetch again current day candle
            # in order to have the 24h-high, 24h-low, and especially 24h-volume updated in real-time
            self.__update_data(symbol_timeframe=self.__btc_usdt_1d)

            self.__print_formatted_output()

        self.__stop_program_if_limit_reached()

    def __stop_program_if_limit_reached(self) -> None:
        """
        Stop the program if we attained the 5 minutes refresh threshold.
        """
        if self.__refresh_count == self.__MAX_REFRESH:
            self.__notifier.stop()

    def __init_data(self) -> None:
        """
        Fetch last available data for defined group of pair/timeframe
        """
        for symbol_timeframe in self.__data_per_symbol_timeframe:
            self.__update_data(symbol_timeframe=symbol_timeframe)

    def __update_data(self, symbol_timeframe: SymbolTimeframe) -> None:
        """
        Update currently stored data for a defined pair/timeframe
        :param symbol_timeframe: pair/timeframe for which we must update data
        """
        number_of_bars: int = symbol_timeframe.get_number_of_candle_per_update()
        must_delete_unfinished_candle: bool = symbol_timeframe.must_delete_unfinished_candle()

        if must_delete_unfinished_candle:
            number_of_bars += 1

        df: pd.DataFrame = ExchangeHelper.get_spot_dataframe(
            exchange=self.__spot_exchange,
            spot_symbol=symbol_timeframe.get_spot_symbol(),
            timeframe=symbol_timeframe.get_timeframe(),
            number_of_candles=number_of_bars,
            delete_last_candle=must_delete_unfinished_candle)

        self.__data_per_symbol_timeframe[symbol_timeframe] = df

    def __print_formatted_output(self) -> None:
        """
        Print formatted output.
        """
        current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(f'[{current_time}]['
              f'current_price={self.__current_price};\t'
              f'24h-High={self.__high_24h};\t'
              f'24h-Low={self.__low_24h};\t'
              f'24h-Volume={self.__volume_24h};\t'
              f'5m-SMA(30)={self.__five_minute_sma}'
              f']')

    @property
    def __current_price(self):
        btc_last_minute_candle: pd.DataFrame = self.__data_per_symbol_timeframe[self.__btc_usdt_1m]
        return btc_last_minute_candle['close'].iloc[0]

    @property
    def __high_24h(self) -> int:
        btc_current_day_candle: pd.DataFrame = self.__data_per_symbol_timeframe[self.__btc_usdt_1d]
        return btc_current_day_candle['high'].iloc[0]

    @property
    def __low_24h(self) -> int:
        btc_current_day_candle: pd.DataFrame = self.__data_per_symbol_timeframe[self.__btc_usdt_1d]
        return btc_current_day_candle['low'].iloc[0]

    @property
    def __volume_24h(self) -> int:
        btc_current_day_candle: pd.DataFrame = self.__data_per_symbol_timeframe[self.__btc_usdt_1d]
        return btc_current_day_candle['volume'].iloc[0]

    @property
    def __five_minute_sma(self) -> float:
        btc_last_30_candles_on_5m_tf: pd.DataFrame = self.__data_per_symbol_timeframe[self.__btc_usdt_5m]
        return btc_last_30_candles_on_5m_tf['close'].mean()


def main():
    real_time_data = RealTimeBitcoinDataPrinter()
    real_time_data.start()


if __name__ == '__main__':
    main()
