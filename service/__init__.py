from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, URL,text
from models.model import Base
from sqlalchemy.pool import QueuePool
import urllib.parse
from utils.logger import app_logger

import os
import sys


def get_engine():
    if getattr(sys, 'frozen', False):
        # 如果是打包后的exe，从exe所在目录加载配置
        config_path = os.path.join(os.path.dirname(sys.executable), 'config', 'database_config.py')
    else:
        # 如果是脚本运行，从当前目录加载配置
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'database_config.py')

    # 动态加载配置文件
    import importlib.util
    spec = importlib.util.spec_from_file_location("database_config", config_path)
    database_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(database_config)

    DATABASES = database_config.DATABASES
    DB_TYPE = database_config.DB_TYPE

    db_config = DATABASES.get(DB_TYPE)

    if not db_config:
        app_logger.error(f"Database configuration for {DB_TYPE} not found.")
        raise ValueError(f"Database configuration for {DB_TYPE} not found.")

    try:
        pool_size = db_config.get("pool_size", 50)
        max_overflow = db_config.get("max_overflow", 100)
        pool_recycle = db_config.get("pool_recycle", 3600)

        if DB_TYPE == 'oracle':
            connection_string = URL.create(
                drivername="oracle+cx_oracle",
                username=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
                query={"service_name": db_config["service_name"]}
            )
        elif DB_TYPE == 'sqlserver':

            trust_cert = db_config.get('trust_server_certificate', True)
            encrypt = db_config.get('encrypt', 'yes')
            timeout = db_config.get('connection_timeout', 50)

            SERVER = db_config["host"]
            USERNAME = db_config["user"]
            PASSWORD = db_config["password"]
            DATABASE = db_config['database']
            DRIVER = "ODBC Driver 17 for SQL Server"

            encoded_un = urllib.parse.quote_plus(USERNAME)
            encoded_pw = urllib.parse.quote_plus(PASSWORD)

            connection_string = (
                f"mssql+pyodbc://{encoded_un}:{encoded_pw}@{SERVER}:1433/{DATABASE}"
                f"?driver={urllib.parse.quote_plus(DRIVER)}" 
                "&encrypt=yes"
                "&trust_server_certificate=no"
                "&login_timeout=30"
            )

            app_logger.info(f"Creating SQL Server connection_string: {connection_string}")

            # connection_string = (
            #     f'mssql+pyodbc://{db_config["user"]}:{db_config["password"]}'
            #     f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
            #     f'?driver=ODBC Driver 17 for SQL Server'
            #     f'&encrypt={encrypt}'
            #     f'&trust_server_certificate={"yes" if trust_cert else "no"}'
            #     f'&timeout={timeout}'
            #     f'&login_timeout={timeout}'
            # )

            # app_logger.info(f"Creating SQL Server engine with host: {db_config['host']}, port: {db_config['port']}")
            # app_logger.info(f"Connection string (without credentials): mssql+pyodbc://"
            #                 f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            #                 f"?driver=ODBC+Driver+17+for+SQL+Server")

        elif DB_TYPE == 'mysql':
            connection_string = URL.create(
                drivername="mysql+pymysql",
                username=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"]
            )
        else:
            app_logger.error(f"Unsupported database type: {DB_TYPE}")
            raise ValueError(f"Unsupported database type: {DB_TYPE}")

        engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,
            connect_args={
                "timeout": timeout,
                "login_timeout": timeout
            }
        )
        app_logger.info(f"Testing connection to {DB_TYPE} database...")
        try:
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            app_logger.info("Database connection test successful.")
        except Exception as e:
            app_logger.error(f"Database connection test failed: {str(e)}")
            raise

        app_logger.info("Database engine created and tested successfully.")
        return engine
    except Exception as e:
        print(f"Error creating database engine: {str(e)}")
        app_logger.exception("Failed to create database engine.")
        raise


#
# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
#
#
# def get_async_engine():
#     from config.database_config import DATABASES, DB_TYPE
#     db_config = DATABASES.get(DB_TYPE)
#
#     if not db_config:
#         app_logger.error(f"Database configuration for {DB_TYPE} not found.")
#         raise ValueError(f"Database configuration for {DB_TYPE} not found.")
#
#     try:
#         pool_size = db_config.get("pool_size", 100)
#         max_overflow = db_config.get("max_overflow", 200)
#         pool_recycle = db_config.get("pool_recycle", 300)
#         pool_pre_ping = db_config.get("pool_pre_ping", True)
#         pool_timeout = db_config.get("pool_timeout", 30)
#
#         engine = create_async_engine(
#             f'mssql+aioodbc://{db_config["user"]}:{db_config["password"]}'
#             f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
#             f'?driver=ODBC+Driver+17+for+SQL+Server'
#             f'&TrustServerCertificate=yes',
#             pool_size=pool_size,
#             max_overflow=max_overflow,
#             pool_recycle=pool_recycle,
#             pool_pre_ping=pool_pre_ping,
#             pool_timeout=pool_timeout
#         )
#
#         return engine
#     except Exception as e:
#         print(f"Error creating database engine: {str(e)}")
#         app_logger.exception("Failed to create database engine.")
#         raise

#
# AsyncSessionLocal = sessionmaker(
#     bind=get_async_engine(),
#     class_=AsyncSession,
#     expire_on_commit=False
# )
#
#
# async def get_AsyncDb():
#     async with AsyncSessionLocal() as session:
#         try:
#             yield session
#         finally:
#             await session.close()


def create_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def get_db():
    db = create_session()
    try:
        yield db
    finally:
        db.close()


try:
    Base.metadata.create_all(get_engine())
    app_logger.info("Database tables created/verified successfully.")
except Exception as e:
    app_logger.error(f"Failed to create/verify database tables: {str(e)}")
    app_logger.exception("Detailed traceback for table creation error:")
    raise
