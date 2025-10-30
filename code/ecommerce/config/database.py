import os

# Docker DB configs
db_config = {
    'dbname': os.getenv('APP_DB_NAME'),
    'user': os.getenv('APP_DB_USER'),
    'password': os.getenv('APP_DB_PASSWORD'),
    'host': os.getenv('APP_DB_HOST'),
    'port': os.getenv('APP_DB_PORT'),
}

# Local DB configs
# db_config = {
#     'dbname': 'ecommerce',
#     'user': 'postgres',
#     'password': 1433,
#     'host': 'localhost',
#     'port': '5432',
# }
