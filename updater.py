import copy, json, pprint, requests, time
import pandas as pd
import numpy as np
from project_constants import *
from sqlalchemy import create_engine

STRUCTURE_COPY = copy.deepcopy(STRUCTURE)


def get_all_symbols():
    """Return dictionary with all symbols that are ETFs or Common/Preferred Stocks for each exchange"""
    url = SYMBOL_LIST_URL + EOD_API_KEY + '&fmt=json'
    response = requests.get(url)
    json_obj = json.loads(response.text)
    symbols_per_exchange = {'NYSE ARCA': [],
                            'NYSE MKT': [],
                            None: []}  # create empty keys to skip eventual KeyError later when deleting

    for ticker in json_obj:
        if ticker['Type'] in TICKER_TYPES:
            if ticker['Exchange'] not in symbols_per_exchange.keys():
                symbols_per_exchange[ticker['Exchange']] = [ticker['Code']]
            else:
                symbols_per_exchange[ticker['Exchange']].append(ticker['Code'])
    symbols_per_exchange['NYSE'].extend(
        symbols_per_exchange['NYSE ARCA'] + symbols_per_exchange['NYSE MKT'] + symbols_per_exchange[None])
    del symbols_per_exchange['NYSE ARCA']
    del symbols_per_exchange['NYSE MKT']
    del symbols_per_exchange[None]
    return symbols_per_exchange


def get_ticker_data_as_json(exchange, tickers, second_try=False):
    """Construct an url that makes an api call for all the given tickers and handle possible errors"""
    url = MAIN_URL + exchange + '?&symbols='
    for ticker in tickers:
        url += ticker
        url += ','
    url = url[:-1]
    url += f'&api_token={EOD_API_KEY}&fmt=json'
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        #  Wait 20 sec then try to get the data again. If response is still not 200 then return False
        print(response.status_code)
        if not second_try:
            time.sleep(20)
            print('Second try to get data')
            return get_ticker_data_as_json(exchange, tickers, second_try=True)
        else:
            return False


def extract_full_market_data(structure_dict, exchange_list):
    """Get data for each ticker and insert in dictionary. Also return False if an error occurs"""
    tickers_per_exchange = get_all_symbols()
    if 'NYSE' in exchange_list:
        exchange_list = ['NYSE']
    for exchange in exchange_list:
        print(exchange)
        tickers = tickers_per_exchange[exchange]
        counter = 1
        iterate_loop = True
        while iterate_loop:
            if len(tickers) > MAX_NUMBER_OF_TICKERS_IN_URL:
                data = get_ticker_data_as_json(exchange, tickers[:MAX_NUMBER_OF_TICKERS_IN_URL])
                tickers = tickers[MAX_NUMBER_OF_TICKERS_IN_URL:]
            else:
                data = get_ticker_data_as_json(exchange, tickers)
                iterate_loop = False
            if data is False:
                return False
            insert_bulk_information(data, structure_dict)
            print(counter)
            counter += 1
    return structure_dict


def insert_bulk_information(data, structure_dict):
    """Insert data into dictionary. Construct TickerID and asset_id columns artificially. Append missing info as None"""
    for stocks_info in data.values():
        ticker = 'st_' + stocks_info['General']['Code']
        for table_name, dictt in stocks_info.items():
            used_columns = set()
            if table_name not in SPECIAL_TABLES:
                if table_name != 'General':
                    structure_dict[table_name]['TickerID'].append(ticker)
                if table_name == 'General':
                    structure_dict[table_name]['asset_id'].append(ticker)
                for key, value in dictt.items():
                    structure_dict[table_name][key].append(value)
                    used_columns.add(key)
                used_columns.add('asset_id') if table_name == 'General' else used_columns.add('TickerID')
                unupdated_keys = set(structure_dict[table_name].keys()) - used_columns
                for unupdated_value in unupdated_keys:  # add None to keys that are missing in the initial data
                    structure_dict[table_name][unupdated_value].append(None)

            else:  # those JSON objects have a JSON object nested inside which requires specific logic
                for sub_table_name, sub_dictt in dictt.items():
                    structure_dict[table_name][sub_table_name]['TickerID'].append(ticker)
                    for key, value in sub_dictt.items():
                        if type(value) == dict:
                            value = json.dumps(value)  # transform dict into json
                        structure_dict[table_name][sub_table_name][key].append(value)
                        used_columns.add(key)
                    used_columns.add('TickerID')
                    unupdated_keys = set(structure_dict[table_name][sub_table_name].keys()) - used_columns
                    for unupdated_value in unupdated_keys:  # add None to keys that are missing in the initial data
                        structure_dict[table_name][sub_table_name][unupdated_value].append(None)


def transform_structure_dict_into_dataframes(structure_dict):
    df_dict = {
        'general': pd.DataFrame(structure_dict['General']),
        'highlights': pd.DataFrame(structure_dict['Highlights']),
        'valuations': pd.DataFrame(structure_dict['Valuation']),
        'technicals': pd.DataFrame(structure_dict['Technicals']),
        'splits_dividends': pd.DataFrame(structure_dict['SplitsDividends']),
        'earnings_last_0': pd.DataFrame(structure_dict['Earnings']['Last_0']),
        'earnings_last_1': pd.DataFrame(structure_dict['Earnings']['Last_1']),
        'earnings_last_2': pd.DataFrame(structure_dict['Earnings']['Last_2']),
        'earnings_last_3': pd.DataFrame(structure_dict['Earnings']['Last_3']),
        'financials_balance_sheet': pd.DataFrame(structure_dict['Financials']['Balance_Sheet']),
        'financials_cash_flow': pd.DataFrame(structure_dict['Financials']['Cash_Flow']),
        'financials_income_statement': pd.DataFrame(structure_dict['Financials']['Income_Statement']),
    }
    return df_dict


class UpdateTables:
    """
        Contains the entire functionality, needed for the comparison between old and new data, the construction and
        execution of SQL queries to insert, update or delete values.
    """

    def __init__(self, connection_str, database_name, exchange_list):
        self.connection_str = connection_str
        self.database_name = database_name
        self.exchange_list = exchange_list
        self.main_table_name = 'general'
        self.db_connection = create_engine(self.connection_str + '/' + self.database_name)
        new_data = extract_full_market_data(STRUCTURE_COPY, self.exchange_list)
        if not new_data:
            print('An error has occurred!')
            return
        self.new_tables = transform_structure_dict_into_dataframes(new_data)
        self.table_names = list(self.new_tables.keys())
        self.old_tables = self.get_tables_from_sql_database()
        self.compare_new_old_tables()
        self.delete_delisted_tickers()
        main_tickers = list(set(self.tickers_to_insert + self.tickers_to_be_updated_per_table[self.main_table_name]))
        self.split_tickers_and_update_sql_tables(self.main_table_name, main_tickers)
        self.table_names.remove(self.main_table_name)  # parent table updated first and removed from list of tables
        for table_name in self.table_names:
            tickers = list(set(self.tickers_to_insert + self.tickers_to_be_updated_per_table[table_name]))
            self.split_tickers_and_update_sql_tables(table_name, tickers)
        new_ticker_logos = self.get_all_logos()
        if len(new_ticker_logos) > 0:
            self.insert_all_logos_in_db(new_ticker_logos)

    def get_tables_from_sql_database(self):
        """Get all data from the sql database and load it as dataframes in dictionary."""
        tables_in_current_database = {}
        table = pd.read_sql_table(self.main_table_name, con=self.db_connection)
        general_table = table[table['Exchange'].isin(self.exchange_list)]
        tickers = list(general_table.asset_id.values)
        tables_in_current_database[self.main_table_name] = general_table
        for table_name in self.table_names:
            if table_name != self.main_table_name:
                table = pd.read_sql_table(table_name, con=self.db_connection)
                table = table.drop(columns=['ID'])  # drop primary key
                table = table[table['TickerID'].isin(tickers)]
                tables_in_current_database[table_name] = table
        return tables_in_current_database

    def compare_new_old_tables(self):
        """Get newly added and delisted tickers. Get tickers that have updated values"""
        new_tickers = self.new_tables[self.main_table_name]
        old_tickers = self.old_tables[self.main_table_name]
        self.tickers_to_insert, self.tickers_to_delete = self.return_new_old_tickers(new_tickers, old_tickers)
        print('Inserting:', len(self.tickers_to_insert), self.tickers_to_insert)
        print('Deleting: ', len(self.tickers_to_delete), self.tickers_to_delete)
        self.tickers_to_be_updated_per_table = {}
        for table_name in self.table_names:
            new_df, old_df = self.reformat_dataframes(self.new_tables[table_name], self.old_tables[table_name],
                                                      table_name)
            if 'UpdatedAt' in new_df.columns:  # drop this column because it is updated too often
                new_df.drop(['UpdatedAt'], axis=1, inplace=True)
                old_df.drop(['UpdatedAt'], axis=1, inplace=True)
            if new_df.shape != old_df.shape:  # tables with different dimensions cannot be compared
                raise ArithmeticError
            if 'financials' in table_name:
                new_df = self.transform_financial_data_into_json(new_df)
            self.tickers_to_be_updated_per_table[table_name] = self.return_tickers_with_changed_values(new_df, old_df)

    @staticmethod
    def return_new_old_tickers(new_data, old_data):
        new_tickers = list(new_data['asset_id'].values)
        old_tickers = list(old_data['asset_id'].values)
        tickers_to_insert = set(new_tickers) - set(old_tickers)
        tickers_to_delete = set(old_tickers) - set(new_tickers)
        return list(tickers_to_insert), list(tickers_to_delete)

    def reformat_dataframes(self, new_df, old_df, table_name):
        """Drop new and old tickers so that both dataframes have the same number of rows and overlapping tickers"""
        index_column = 'asset_id' if table_name == self.main_table_name else 'TickerID'
        new_df = new_df.set_index(index_column).sort_index()
        old_df = old_df.set_index(index_column).sort_index()
        new_df.drop(self.tickers_to_insert, inplace=True)
        old_df.drop(self.tickers_to_delete, inplace=True)
        return new_df, old_df

    @staticmethod
    def transform_financial_data_into_json(new_data):
        """Transform the values of the last 3 tables (financials) into JSON"""
        columns = list(new_data.columns)
        for col in columns[1:]:  # only first column is not JSON like
            new_data[col] = new_data[col].apply(json.loads)
        return new_data

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
        print(len(tickers_to_be_changed))
        if len(tickers_to_be_changed) == 0:
            return
        table = self.new_tables[table_name]
        columns = list(table.columns)
        insert_sql_query = f'INSERT INTO {self.database_name}.{table_name} ('
        for column in columns:  # append column to raw string
            insert_sql_query += column
            insert_sql_query += ', '
        insert_sql_query = insert_sql_query[:-2]
        insert_sql_query += ') VALUES ('
        for ticker in list(tickers_to_be_changed):  # append new values for each ticker
            values = table.loc[table['TickerID'] == ticker].values[0] if table_name != self.main_table_name else \
                table.loc[table['asset_id'] == ticker].values[0]
            for value in values:
                if pd.isnull(value):
                    insert_sql_query += f'NULL, '  # add SQL NULL instead of python None
                elif type(value) == str:
                    value = value.replace("'", "''")
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

    def get_all_logos(self, ):
        """Append tuples (asset_id, logo_url) for all newly added tickers (logo url cannot be currently updated!)"""
        table = pd.read_sql_table('logos', con=self.db_connection)
        available_tickers = list(table['TickerID'].values)
        new_tickers = list(set(self.tickers_to_insert) - set(available_tickers))
        try:
            asset_id_logo_tuples = []
            for ticker in new_tickers:
                asset_id_logo_tuples.append(self.get_single_ticker_logo(ticker))
            print('Logos extracted')
            return asset_id_logo_tuples
        except requests.exceptions.ConnectionError:
            print('Sleeping')
            time.sleep(15)
            return self.get_all_logos()

    @staticmethod
    def get_single_ticker_logo(ticker):
        """Try to get the logo url for ticker with upper and lower letters. If both responses are != 200, return None"""
        ticker = ticker.split('_')[1]
        url = f'https://eodhistoricaldata.com/img/logos/US/{ticker}.png'
        response = requests.get(url)
        if response.status_code == 200:
            return 'st_' + ticker, url
        else:
            ticker_lower = ticker.lower()
            url = f'https://eodhistoricaldata.com/img/logos/US/{ticker_lower}.png'
            response = requests.get(url)
            if response.status_code == 200:
                return 'st_' + ticker, url
            else:
                return 'st_' + ticker, None

    def construct_execute_insert_query_logos(self, new_ticker_logos):
        """Insert logos for up to 999 ticker in table logos in database"""
        insert_sql_query = f'INSERT INTO {self.database_name}.logos (TickerID, LogoUrl) VALUES '
        for tpl in new_ticker_logos:
            insert_sql_query += f'(\'{tpl[0]}\', \'{tpl[1]}\'),' if tpl[1] is not None else f'(\'{tpl[0]}\', NULL),'
        insert_sql_query = insert_sql_query[:-1]
        insert_sql_query += ';'
        self.db_connection.execute(insert_sql_query)
        print('Logos inserted:', len(new_ticker_logos))

    def insert_all_logos_in_db(self, new_ticker_logos):
        """Split new_ticker_logos into chunks of 999 values and construct/execute insert query for each chunk."""
        while len(new_ticker_logos) > MAX_NUMBER_OF_TICKERS_PER_QUERY:
            self.construct_execute_insert_query_logos(list[:MAX_NUMBER_OF_TICKERS_PER_QUERY])
            new_ticker_logos = new_ticker_logos[MAX_NUMBER_OF_TICKERS_PER_QUERY:]
        self.construct_execute_insert_query_logos(new_ticker_logos)
