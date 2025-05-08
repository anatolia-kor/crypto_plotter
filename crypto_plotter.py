import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime as dt


class Plotter(object):

    def __init__(self, ticker, trade_data_path, hist_data_path, date_format_tx, date_format_hist):
        self.ticker = ticker
        self.trade_data_path = trade_data_path
        self.hist_data_path = f'hist_data/{hist_data_path}'
        self.raw_trade_data = pd.DataFrame
        self.date_format_tx = date_format_tx
        self.date_format_hist = date_format_hist
        self.raw_hist_data = pd.DataFrame
        self.bought_data = pd.DataFrame
        self.sell_data = pd.DataFrame
        self.reb_in_data = pd.DataFrame
        self.reb_out_data = pd.DataFrame
        self.hist_data = pd.DataFrame
        self.cost_data = pd.DataFrame

    def read_raw_data(self):
        self.raw_hist_data = pd.read_csv(self.hist_data_path)
        self.raw_trade_data = pd.read_excel(self.trade_data_path)

    def prepare_data(self):

        # trading data
        self.bought_data = self.raw_trade_data[(self.raw_trade_data['ticker_in'] == self.ticker) |
                                               (self.raw_trade_data['ticker_out'] == self.ticker)]
        min_data = self.tx_to_dtime(self.bought_data['date'].min())
        self.sell_data = self.bought_data.copy()
        self.reb_in_data = self.bought_data.copy()
        self.reb_out_data = self.bought_data.copy()
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
        self.bought_data['quantity'] = (100 / self.bought_data['quantity'].max()) * self.bought_data['quantity']
        self.bought_data['date'] = self.bought_data['date'].apply(self.tx_to_dtime)

        # sell data
        self.sell_data = self.sell_data.apply(self.is_sell, axis=1).dropna()
        self.sell_data = self.sell_data[['date', 'count_in', 'count_out']].reset_index(drop=True)
        self.sell_data['count_in'] = self.sell_data['count_in'].astype(float)
        self.sell_data['count_out'] = self.sell_data['count_out'].astype(float)
        self.sell_data['price'] = self.sell_data['count_in'] / self.sell_data['count_out']
        self.sell_data = self.sell_data.drop(columns=['count_in'])
        self.sell_data = self.sell_data.rename(columns={'count_out': 'quantity'})
        self.sell_data['quantity'] = (100 / self.sell_data['quantity'].max()) * self.sell_data['quantity']
        self.sell_data['date'] = self.sell_data['date'].apply(self.tx_to_dtime)

        # historical data
        self.hist_data = self.raw_hist_data[['Date', 'Close']]
        self.hist_data = self.hist_data[::-1].reset_index(drop=True)
        self.hist_data['Close'] = self.hist_data['Close'].astype(float)
        self.hist_data['Date'] = self.hist_data['Date'].apply(self.hist_to_dtime)
        self.hist_data = self.hist_data.rename(columns={'Date': 'date', 'Close': 'price'})
        if min_data in [d for d in self.hist_data['date']]:
            index_first_interact = self.hist_data.loc[self.hist_data['date'] == min_data].index.item()
        else:
            index_first_interact = 0
        if index_first_interact > 50:
            index_first_interact = index_first_interact - 50
        self.hist_data = self.hist_data[index_first_interact:]

        # rebalance in data
        self.reb_in_data = self.reb_in_data.apply(self.is_rebalancing_in, axis=1).dropna()
        self.reb_in_data = self.reb_in_data[['date', 'count_in', 'count_out']].reset_index(drop=True)
        self.reb_in_data['count_in'] = self.reb_in_data['count_in'].astype(float)
        self.reb_in_data = self.reb_in_data.drop(columns=['count_out'])
        self.reb_in_data = self.reb_in_data.rename(columns={'count_in': 'quantity'})
        self.reb_in_data['quantity'] = (100 / self.reb_in_data['quantity'].max()) * self.reb_in_data['quantity']
        self.reb_in_data['date'] = self.reb_in_data['date'].apply(self.tx_to_dtime)
        self.reb_in_data = self.reb_in_data.merge(self.hist_data[['date', 'price']],
                                                  left_on='date', right_on='date', how='inner')

        # self cost
        self.cost_data = self.cost_data.apply(self.is_cost_changer, axis=1).dropna()
        self.cost_data = self.cost_data[['date', 'cost']].reset_index(drop=True)
        self.cost_data['cost'] = self.cost_data['cost'].astype(float)
        self.cost_data['date'] = self.cost_data['date'].apply(self.tx_to_dtime)
        self.cost_data = pd.concat([self.cost_data,
                                    pd.DataFrame({'date': self.hist_data[::-1].iloc[0]['date'],
                                                  'cost': [self.cost_data[::-1].iloc[0]['cost']]
                                                  })
                                    ]).reset_index(drop=True)

        # rebalance out data
        self.reb_out_data = self.reb_out_data.apply(self.is_rebalancing_out, axis=1).dropna()
        self.reb_out_data = self.reb_out_data[['date', 'count_in', 'count_out']].reset_index(drop=True)
        self.reb_out_data['count_out'] = self.reb_out_data['count_out'].astype(float)
        self.reb_out_data = self.reb_out_data.drop(columns=['count_in'])
        self.reb_out_data = self.reb_out_data.rename(columns={'count_out': 'quantity'})
        self.reb_out_data['quantity'] = (100 / self.reb_out_data['quantity'].max()) * self.reb_out_data['quantity']
        self.reb_out_data['date'] = self.reb_out_data['date'].apply(self.tx_to_dtime)
        self.reb_out_data = self.reb_out_data.merge(self.hist_data[['date', 'price']],
                                                    left_on='date', right_on='date', how='inner')

        self.hist_data = self.hist_data.set_index(self.hist_data['date']).drop(['date'], axis=1)

    def plot(self):
        plt.figure(self.ticker, figsize=(16, 6))
        plt.title(self.ticker)
        plt.xlabel('time')
        plt.ylabel('price')
        plt.plot(self.hist_data, label='historical price')
        plt.step(self.cost_data['date'], self.cost_data['cost'], color='black', where='post', label='self cost')
        plt.scatter(x=self.bought_data['date'], y=self.bought_data['price'],
                    s=self.bought_data['quantity'], c='green', label='buy points')
        plt.scatter(x=self.reb_in_data['date'], y=self.reb_in_data['price'],
                    s=self.reb_in_data['quantity'], c='yellow', label='rebalancing from another crypto')
        plt.scatter(x=self.sell_data['date'], y=self.sell_data['price'],
                    s=self.sell_data['quantity'], c='red', label='sell points')
        plt.scatter(x=self.reb_out_data['date'], y=self.reb_out_data['price'],
                    s=self.reb_out_data['quantity'], c='purple', label='rebalancing in another crypto')
        plt.legend(loc="upper left")
        plt.savefig(f'chart/{self.ticker}.png')
        plt.show()

    def tx_to_dtime(self, str_dt):
        return dt.strptime(str_dt, self.date_format_tx)

    def hist_to_dtime(self, str_dt):
        return dt.strptime(str_dt, self.date_format_hist)

    def is_bought_condition(self, row):
        return row['ticker_in'] == self.ticker and 'USD' in row['ticker_out']

    def is_rebalancing_in_condition(self, row):
        return (row['ticker_in'] == self.ticker and
                'USD' not in row['ticker_out'] and
                row['ticker_in'] != row['ticker_out'] and
                row['ticker_out'] != '<empty>')

    def is_sell_condition(self, row):
        return row['ticker_out'] == self.ticker and 'USD' in row['ticker_in']

    def is_rebalancing_out_condition(self, row):
        return (row['ticker_out'] == self.ticker and
                'USD' not in row['ticker_in'] and
                row['ticker_in'] != row['ticker_out'] and
                row['ticker_in'] != '<empty>')

    def is_bought(self, row):
        return row if self.is_bought_condition(row) else row.apply(lambda x: pd.NA)

    def is_rebalancing_in(self, row):
        return row if self.is_rebalancing_in_condition(row) else row.apply(lambda x: pd.NA)

    def is_cost_changer(self, row):
        return row if (self.is_bought_condition(row) or self.is_rebalancing_in_condition(row)) \
            else row.apply(lambda x: pd.NA)

    def is_sell(self, row):
        return row if self.is_sell_condition(row) else row.apply(lambda x: pd.NA)

    def is_rebalancing_out(self, row):
        return row if self.is_rebalancing_out_condition(row) else row.apply(lambda x: pd.NA)


def main():
    harry_plotter = Plotter('ALGO',
                            'transactions.xlsx',
                            'Binance_ALGOUSDT_d.csv',
                            '%Y-%m-%d',
                            '%Y-%m-%d')
    # '%Y-%m-%d'
    # '%m/%d/%Y'
    harry_plotter.read_raw_data()
    harry_plotter.prepare_data()
    harry_plotter.plot()


if __name__ == '__main__':
    main()
