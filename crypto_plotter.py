import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime as dt


class Plotter(object):

    def __init__(self, ticker, trade_data_path, hist_data_path):
        self.ticker = ticker
        self.trade_data_path = trade_data_path
        self.hist_data_path = hist_data_path
        self.raw_trade_data = pd.DataFrame
        self.raw_hist_data = pd.DataFrame
        self.bought_data = pd.DataFrame
        self.sell_data = pd.DataFrame
        self.hist_data = pd.DataFrame
        self.cost_data = pd.DataFrame

    def read_raw_data(self):
        self.raw_hist_data = pd.read_csv(self.hist_data_path)
        self.raw_trade_data = pd.read_excel(self.trade_data_path)

    def prepare_data(self):

        # trading data
        self.bought_data = self.raw_trade_data[(self.raw_trade_data['ticker_in'] == self.ticker) |
                                               (self.raw_trade_data['ticker_out'] == self.ticker)]
        min_data = self.bought_data['date'].min()
        self.sell_data = self.bought_data.copy()
        self.cost_data = self.bought_data.copy()

        # bought data
        self.bought_data = self.bought_data.apply(self.is_bought, axis=1).dropna()
        self.bought_data = self.bought_data[['date', 'count_in', 'count_out']].reset_index(drop=True)
        self.bought_data['count_in'] = self.bought_data['count_in'].astype(float)
        self.bought_data['count_out'] = self.bought_data['count_out'].astype(float)
        self.bought_data['price'] = self.bought_data['count_out'] / self.bought_data['count_in']
        self.bought_data = self.bought_data.drop(columns=['count_out'])
        self.bought_data = self.bought_data.rename(columns={'count_in': 'quantity'})
        self.bought_data['date'] = self.bought_data['date'].apply(self.to_dtime)

        # sell data
        self.sell_data = self.sell_data.apply(self.is_sell, axis=1).dropna()
        self.sell_data = self.sell_data[['date', 'count_in', 'count_out']].reset_index(drop=True)
        self.sell_data['count_in'] = self.sell_data['count_in'].astype(float)
        self.sell_data['count_out'] = self.sell_data['count_out'].astype(float)
        self.sell_data['price'] = self.sell_data['count_in'] / self.sell_data['count_out']
        self.sell_data = self.sell_data.drop(columns=['count_in'])
        self.sell_data = self.sell_data.rename(columns={'count_out': 'quantity'})
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
        self.hist_data = self.hist_data.set_index(self.hist_data['date']).drop(['date'], axis=1)

        # self cost
        self.cost_data = self.cost_data.apply(self.is_bought, axis=1).dropna()
        self.cost_data = self.cost_data[['date', 'cost']].reset_index(drop=True)
        self.cost_data['cost'] = self.cost_data['cost'].astype(float)
        self.cost_data['date'] = self.cost_data['date'].apply(self.to_dtime)
        self.cost_data = pd.concat([self.cost_data,
                                    pd.DataFrame({'date': self.hist_data.index[::-1][0],
                                                  'cost': [self.cost_data[::-1].iloc[0]['cost']]
                                                  })
                                    ]).reset_index(drop=True)

    def plot(self):
        fig = plt.figure(self.ticker, figsize=(16, 6))
        plt.title(self.ticker)
        plt.xlabel('time')
        plt.ylabel('price')
        plt.plot(self.hist_data)
        plt.step(self.cost_data['date'], self.cost_data['cost'], color='black', where='post')
        plt.scatter(x=self.bought_data['date'], y=self.bought_data['price'],
                    s=self.bought_data['quantity'] * 1000, c='green')
        plt.scatter(x=self.sell_data['date'], y=self.sell_data['price'],
                    s=self.sell_data['quantity'] * 10, c='green')
        plt.savefig(f'{self.ticker}.png')
        plt.show()

    @staticmethod
    def to_dtime(str_dt):
        return dt.strptime(str_dt, '%Y-%m-%d')

    def is_bought(self, row):
        return row if row['ticker_in'] == self.ticker and 'USD' in row['ticker_out'] else row.apply(lambda x: pd.NA)

    def is_sell(self, row):
        return row if row['ticker_out'] == self.ticker and 'USD' in row['ticker_in'] else row.apply(lambda x: pd.NA)


def main():
    harry_plotter = Plotter('ETH', 'transactions.xlsx', 'Binance_ETHUSDT_d.csv')
    harry_plotter.read_raw_data()
    harry_plotter.prepare_data()
    harry_plotter.plot()


if __name__ == '__main__':
    main()
