import requests, json, copy
import pandas as pd
import numpy as np
from project_constants import *
from sqlalchemy import create_engine


def get_forex_indices_data():
    resp = requests.get(FOREX_URL)
    forex_data = json.loads(resp.text)['data']
    resp_in = requests.get(INDICES_URL)
    indices_data = json.loads(resp_in.text)['data']

    unique_symbols = []
    forex_struct = copy.deepcopy(FOREX_TABLE_STRUCTURE)
    forex_pairs_struct = copy.deepcopy(FOREX_PAIRS_TABLE_STRUCTURE)
    indices_struct = copy.deepcopy(INDICES_TABLE_STRUCTURE)

    for pair in forex_data:
        asset = pair['symbol'].split('/')[0]
        if asset not in unique_symbols:
            unique_symbols.append(asset)
            forex_struct['asset_id'].append('fo_' + asset)
            forex_struct['symbol'].append(asset)
            forex_struct['name'].append(pair['currency_base'])
        forex_pairs_struct['asset_id'].append('fo_' + pair['symbol'].replace('/', ''))
        forex_pairs_struct['symbol'].append(pair['symbol'])
        forex_pairs_struct['currency_group'].append(pair['currency_group'])
        forex_pairs_struct['currency_base'].append(pair['currency_base'])
        forex_pairs_struct['currency_quote'].append(pair['currency_quote'])

    for index in indices_data:
        indices_struct['asset_id'].append('in_' + index['symbol'])
        indices_struct['symbol'].append(index['symbol'])
        indices_struct['name'].append(index['name'])
        indices_struct['country'].append(index['country'])
        indices_struct['currency'].append(index['currency'])

    return pd.DataFrame(forex_struct), pd.DataFrame(forex_pairs_struct), pd.DataFrame(indices_struct)


class UpdateForexIndices:
    """
        Contains the entire functionality, needed for the comparison between old and new data, the construction and
        execution of SQL queries to insert, update or delete values.
    """

    def __init__(self, connection_str, database_name):
        self.connection_str = connection_str
        self.database_name = database_name
        self.db_connection = create_engine(self.connection_str + '/' + self.database_name)
        self.table_names = ['forex', 'forex_pairs', 'indices']
        new_data = get_forex_indices_data()
        self.new_tables_data = {
            self.table_names[idx]: new_data[idx] for idx in range(len(new_data))
        }
        print("New data extracted")
        old_data = self.get_table_from_sql_database()
        self.old_tables_data = {
            self.table_names[idx]: old_data[idx] for idx in range(len(old_data))
        }
        print("Old data extracted")
        self.compare_new_old_tables()
        for table_name in self.table_names:
            delete_tickers = self.tickers_to_delete[table_name]
            insert_tickers = self.tickers_to_insert[table_name]
            self.delete_delisted_tickers(table_name, delete_tickers)
            self.split_tickers_and_update_sql_tables(table_name, insert_tickers)

    def get_table_from_sql_database(self):
        """Get all data from the sql database and load it as dataframe."""
        table_1 = pd.read_sql_table(self.table_names[0], con=self.db_connection)
        table_2 = pd.read_sql_table(self.table_names[1], con=self.db_connection)
        table_3 = pd.read_sql_table(self.table_names[2], con=self.db_connection)
        return table_1, table_2, table_3

    def compare_new_old_tables(self):
        """Get newly added and delisted tickers. Get tickers that have updated values"""
        self.tickers_to_delete = {}
        self.tickers_to_insert = {}
        for table_name in self.table_names:
            new_data, old_table = self.new_tables_data[table_name], self.old_tables_data[table_name]
            self.tickers_to_delete[table_name], self.tickers_to_insert[table_name] = self.return_old_new_tickers(
                new_data, old_table)
            print('Deleting: ', len(self.tickers_to_delete), self.tickers_to_delete)
            print('New tickers:', len(self.tickers_to_insert), self.tickers_to_insert)
            new_df, old_df = self.reformat_dataframes(table_name, new_data, old_table)
            if new_df.shape != old_df.shape:  # tables with different dimensions cannot be compared
                raise ArithmeticError
            self.tickers_to_insert[table_name].extend(self.return_tickers_with_changed_values(new_df, old_df))

    @staticmethod
    def return_old_new_tickers(new_data, old_data):
        old_tickers = list(old_data['asset_id'].values)
        new_tickers = list(new_data['asset_id'].values)
        tickers_to_delete = set(old_tickers) - set(new_tickers)
        tickers_to_insert = set(new_tickers) - set(old_tickers)
        return list(tickers_to_delete), list(tickers_to_insert)

    def reformat_dataframes(self, table_name, new_df, old_df):
        """Drop new and old tickers so that both dataframes have the same number of rows and overlapping tickers"""
        new_df = new_df.set_index('asset_id').sort_index()
        old_df = old_df.set_index('asset_id').sort_index()
        new_df.drop(self.tickers_to_insert[table_name], inplace=True)
        old_df.drop(self.tickers_to_delete[table_name], inplace=True)
        return new_df, old_df

    @staticmethod
    def return_tickers_with_changed_values(new_df, old_df):
        """
            Compare both tables and return the tickers whose values have been changed. Since np.nan != np.nan, a
            simple comparison is not enough. In the end we get the index/columns of updated (different) values and
            return the tickers that have to be updated for this specific table.
        """
        differences_table = new_df == old_df
        both_nan_values = pd.isnull(new_df).replace(False, np.nan) == pd.isnull(old_df).replace(False, np.nan)
        table_with_differences = differences_table == both_nan_values
        idx_col_with_differences = table_with_differences[table_with_differences == True].stack().index.tolist()
        unique_tickers_to_be_updated = set([tpl[0] for tpl in idx_col_with_differences])
        return list(unique_tickers_to_be_updated)

    def split_tickers_and_update_sql_tables(self, table_name, tickers):
        """Split tickers into chunks of 999. Construct and execute SQL queries for each chunk recursively."""
        if len(tickers) < MAX_NUMBER_OF_TICKERS_PER_QUERY:
            self.construct_execute_insert_on_duplicate_sql_query(table_name, tickers)
            return
        else:
            self.construct_execute_insert_on_duplicate_sql_query(table_name, tickers[:MAX_NUMBER_OF_TICKERS_PER_QUERY])
            tickers = tickers[MAX_NUMBER_OF_TICKERS_PER_QUERY:]
            self.split_tickers_and_update_sql_tables(table_name, tickers)

    def construct_execute_insert_on_duplicate_sql_query(self, table_name, tickers_to_be_changed):
        """Create a raw sql query as string with updated values for all given tickers and execute that query"""
        if len(tickers_to_be_changed) == 0:
            return
        print(len(tickers_to_be_changed))
        table = self.new_tables_data[table_name]
        columns = list(table.columns)
        insert_sql_query = f'INSERT INTO {self.database_name}.{table_name} ('
        for column in columns:  # append column to raw string
            insert_sql_query += column
            insert_sql_query += ', '
        insert_sql_query = insert_sql_query[:-2]
        insert_sql_query += ') VALUES ('
        for ticker in list(tickers_to_be_changed):  # append new values for each ticker
            values = table.loc[table['asset_id'] == ticker].values[0]
            for value in values:
                if pd.isnull(value):
                    insert_sql_query += f'NULL, '  # add SQL NULL instead of python None
                elif type(value) == str:
                    value = value.replace("'", "''")
                    insert_sql_query += f'\'{value}\', '  # add quotes to string values
                elif type(value) == bool:
                    insert_sql_query += f'\'{value}\', '  # add quotes to string values
                else:
                    insert_sql_query += f'{value}, '
            insert_sql_query = insert_sql_query[:-2]
            insert_sql_query += '), ('
        insert_sql_query = insert_sql_query[:-3]
        insert_sql_query += ' ON DUPLICATE KEY UPDATE '
        for column in columns:  # append last part of the query
            insert_sql_query += f'{column} = VALUES({column})'
            insert_sql_query += ', '
        insert_sql_query = insert_sql_query[:-2]
        insert_sql_query += ';'
        self.db_connection.execute(insert_sql_query)  # execute the raw sql query
        print(table_name + ' updated!')

    def delete_delisted_tickers(self, table_name, tickers):
        """
            Construct and execute the SQL query that deletes tickers in main table. Other tables' rows are deleted
            through their foreign key on cascade.
        """
        if len(tickers) == 0:
            return
        delete_sql_query = 'DELETE FROM {}.{} WHERE Symbol IN ('.format(self.database_name, table_name)
        for ticker in tickers:
            delete_sql_query += '\'{}\', '.format(ticker)
        delete_sql_query = delete_sql_query[:-2]
        delete_sql_query += ');'
        self.db_connection.execute(delete_sql_query)
        print("Deleted")
        print(tickers)
