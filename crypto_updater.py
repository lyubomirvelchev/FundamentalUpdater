import json, copy
import requests
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from project_constants import *


def get_crypto_data(url):
    response = requests.get(url)
    return json.loads(response.text)


def create_dataframe_from_crypto_data(structure_dict):
    data = get_crypto_data(CRYPTO_URL)
    for crypto_info in data['Data'].values():
        used_columns = set()
        for key, value in crypto_info.items():
            if key not in CRYPTO_SPECIAL_COLUMNS:
                structure_dict[key].append(value)
                used_columns.add(key)
            elif key == 'Symbol':
                structure_dict[key].append('cr_' + value)
                used_columns.add(key)
            else:
                if key == 'Taxonomy':
                    for field, string in value.items():
                        column = 'Taxonomy_' + field
                        structure_dict[column].append(string)
                        used_columns.add(column)
                else:
                    for field, string in value['Weiss'].items():
                        column = 'Weiss_' + field
                        structure_dict[column].append(string)
                        used_columns.add(column)
        unupdated_keys = set(structure_dict.keys()) - used_columns
        for unupdated_value in unupdated_keys:  # add None to keys that are missing in the initial data
            structure_dict[unupdated_value].append(None)
    return pd.DataFrame(structure_dict)


class UpdateCryptoTable:
    """
        Contains the entire functionality, needed for the comparison between old and new data, the construction and
        execution of SQL queries to insert, update or delete values.
    """

    def __init__(self, connection_str, database_name):
        self.connection_str = connection_str
        self.database_name = database_name
        self.main_table_name = 'crypto_assets'
        self.db_connection = create_engine(self.connection_str + '/' + self.database_name)
        structure_dict = copy.deepcopy(CRYPTO_STRUCTURE_DICT)
        self.new_data = create_dataframe_from_crypto_data(structure_dict)
        print("New data extracted")
        self.old_table = self.get_table_from_sql_database()
        print("Old data extracted")
        self.compare_new_old_tables()
        self.delete_delisted_tickers()
        tickers = list(set(self.tickers_to_insert + self.tickers_to_be_updated))
        self.split_tickers_and_update_sql_tables(tickers)

    def get_table_from_sql_database(self):
        """Get all data from the sql database and load it as dataframe."""
        return pd.read_sql_table(self.main_table_name, con=self.db_connection)

    def compare_new_old_tables(self):
        """Get newly added and delisted tickers. Get tickers that have updated values"""
        self.tickers_to_insert, self.tickers_to_delete = self.return_new_old_tickers(self.new_data, self.old_table)
        print('New tickers:', len(self.tickers_to_insert), self.tickers_to_insert)
        print('Deleting: ', len(self.tickers_to_delete), self.tickers_to_delete)
        new_df, old_df = self.reformat_dataframes(self.new_data, self.old_table)
        if new_df.shape != old_df.shape:  # tables with different dimensions cannot be compared
            raise ArithmeticError
        self.tickers_to_be_updated = self.return_tickers_with_changed_values(new_df, old_df)

    @staticmethod
    def return_new_old_tickers(new_data, old_data):
        new_tickers = list(new_data['Symbol'].values)
        old_tickers = list(old_data['Symbol'].values)
        tickers_to_insert = set(new_tickers) - set(old_tickers)
        tickers_to_delete = set(old_tickers) - set(new_tickers)
        return list(tickers_to_insert), list(tickers_to_delete)

    def reformat_dataframes(self, new_df, old_df):
        """Drop new and old tickers so that both dataframes have the same number of rows and overlapping tickers"""
        new_df = new_df.set_index('Symbol').sort_index()
        old_df = old_df.set_index('Symbol').sort_index()
        new_df['Sponsored'] = new_df['Sponsored'].apply(str)
        new_df['IsTrading'] = new_df['IsTrading'].apply(str)
        new_df.drop(self.tickers_to_insert, inplace=True)
        old_df.drop(self.tickers_to_delete, inplace=True)
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

    def split_tickers_and_update_sql_tables(self, tickers):
        """Split tickers into chunks of 999. Construct and execute SQL queries for each chunk recursively."""
        if len(tickers) < MAX_NUMBER_OF_TICKERS_PER_QUERY:
            self.construct_execute_insert_on_duplicate_sql_query(tickers)
            return
        else:
            self.construct_execute_insert_on_duplicate_sql_query(tickers[:MAX_NUMBER_OF_TICKERS_PER_QUERY])
            tickers = tickers[MAX_NUMBER_OF_TICKERS_PER_QUERY:]
            self.split_tickers_and_update_sql_tables(tickers)

    def construct_execute_insert_on_duplicate_sql_query(self, tickers_to_be_changed):
        """Create a raw sql query as string with updated values for all given tickers and execute that query"""
        print(len(tickers_to_be_changed))
        if len(tickers_to_be_changed) == 0:
            return
        table = self.new_data
        columns = list(table.columns)
        insert_sql_query = f'INSERT INTO {self.database_name}.{self.main_table_name} ('
        for column in columns:  # append column to raw string
            insert_sql_query += column
            insert_sql_query += ', '
        insert_sql_query = insert_sql_query[:-2]
        insert_sql_query += ') VALUES ('
        for ticker in list(tickers_to_be_changed):  # append new values for each ticker
            values = table.loc[table['Symbol'] == ticker].values[0]
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
        print(self.main_table_name + ' updated!')

    def delete_delisted_tickers(self):
        """
            Construct and execute the SQL query that deletes tickers in main table. Other tables' rows are deleted
            through their foreign key on cascade.
        """
        if not self.tickers_to_delete:
            return
        delete_sql_query = 'DELETE FROM {}.general WHERE asset_id IN ('.format(self.database_name)
        for ticker in self.tickers_to_delete:
            delete_sql_query += '\'{}\', '.format(ticker)
        delete_sql_query = delete_sql_query[:-2]
        delete_sql_query += ');'
        self.db_connection.execute(delete_sql_query)
        print("Deleted")
        print(self.tickers_to_delete)
