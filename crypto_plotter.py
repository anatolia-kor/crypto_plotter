import pandas as pd
from datetime import datetime as dt


class Plotter(object):

    def __init__(self, ticker):
        self.ticker = ticker
        self.trade_data_path = 'transactions.xlsx'
        self.hist_data_path = 'Binance_ETHUSDT_d.csv'
        self.raw_trade_data = pd.DataFrame
        self.raw_hist_data = pd.DataFrame
        self.bought_data = pd.DataFrame
        self.sell_data = pd.DataFrame
        self.hist_data = pd.DataFrame

    def read_raw_data(self):
        self.raw_hist_data = pd.read_csv(self.hist_data_path)
        self.raw_trade_data = pd.read_excel(self.trade_data_path)

    def prepare_data(self):

        # trading data
        self.bought_data = self.raw_trade_data[(self.raw_trade_data['ticker_in'] == self.ticker) |
                                               (self.raw_trade_data['ticker_out'] == self.ticker)]
        min_data = self.bought_data['date'].min()
        self.sell_data = self.bought_data.copy()

        self.bought_data = self.bought_data.apply(self.is_bought, axis=1).dropna()
        self.bought_data = self.bought_data[['date', 'count_in']].reset_index(drop=True)
        self.bought_data['count_in'] = self.bought_data['count_in'].astype(float)
        self.bought_data['date'] = self.bought_data['date'].apply(self.to_dtime)

        self.sell_data = self.sell_data.apply(self.is_sell, axis=1).dropna()
        self.sell_data = self.sell_data[['date', 'ticker_out']].reset_index(drop=True)
        self.sell_data['ticker_out'] = self.sell_data['ticker_out'].astype(float)
        self.sell_data['date'] = self.sell_data['date'].apply(self.to_dtime)

        # historical data
        self.hist_data = self.raw_hist_data[['Date', 'Close']]
        self.hist_data = self.hist_data[::-1].reset_index(drop=True)
        self.hist_data['Close'] = self.hist_data['Close'].astype(float)
        self.hist_data['Date'] = self.hist_data['Date'].apply(self.to_dtime)
        self.hist_data = self.hist_data.rename(columns={'Date': 'date', 'Close': 'price'})
        index_first_interact = self.hist_data.loc[self.hist_data['date'] == min_data].index.item()
        if index_first_interact > 50:
            index_first_interact = index_first_interact - 50
        self.hist_data = self.hist_data[index_first_interact:]

    @staticmethod
    def to_dtime(str_dt):
        return dt.strptime(str_dt, '%Y-%m-%d')

    def is_bought(self, row):
        return row if row['ticker_in'] == self.ticker and 'USD' in row['ticker_out'] else row.apply(lambda x: pd.NA)

    def is_sell(self, row):
        return row if row['ticker_out'] == self.ticker and 'USD' in row['ticker_in'] else row.apply(lambda x: pd.NA)


def main():
    plt = Plotter('ETH')
    plt.read_raw_data()
    plt.prepare_data()


if __name__ == '__main__':
    main()
