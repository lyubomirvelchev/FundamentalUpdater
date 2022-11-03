from updater import UpdateTables
from crypto_updater import UpdateCryptoTable
from project_constants import EXCHANGE_LIST_V1, EXCHANGE_LIST_V2, EXCHANGE_LIST_V3, EXCHANGE_LIST_V4


def update_tables():
    aws_connection = 'mysql+mysqlconnector://admin:dsj89jdj!li3dj2ljefds@database-1.clns6yopmify.us-east-1.rds.amazonaws.com:3306'
    stocks_schema_name = 'stock_etf_fundamentals'
    crypto_schema_name = 'crypto_fundamentals'
    UpdateCryptoTable(aws_connection, crypto_schema_name)
    UpdateTables(aws_connection, stocks_schema_name, EXCHANGE_LIST_V1)
    UpdateTables(aws_connection, stocks_schema_name, EXCHANGE_LIST_V2)
    UpdateTables(aws_connection, stocks_schema_name, EXCHANGE_LIST_V3)
    UpdateTables(aws_connection, stocks_schema_name, EXCHANGE_LIST_V4)


if __name__ == '__main__':
    update_tables()
