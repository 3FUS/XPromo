DATABASES = {
    'oracle': {
        'host': 'localhost',
        'port': 1521,
        'user': 'user',
        'password': 'password',
        'service_name': 'service_name'
    },
    'sqlserver': {
        'host': '192.168.0.32',
        'port': 1433,
        'user': 'sa',
        'password': 'Xstore123',
        'database': 'xcenter',
        'trust_server_certificate': True,
        'encrypt': 'yes'
    },
    'mysql': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'rootroot',
        'database': 'datahub_dev'
    }
}

DB_TYPE = 'sqlserver'
