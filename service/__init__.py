from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, URL
from models.model import Base
from sqlalchemy.pool import QueuePool

from utils.logger import setup_logger

logger = setup_logger(__name__)


def get_engine():
    from config.database_config import DATABASES, DB_TYPE
    db_config = DATABASES.get(DB_TYPE)

    if not db_config:
        logger.error(f"Database configuration for {DB_TYPE} not found.")
        raise ValueError(f"Database configuration for {DB_TYPE} not found.")

    try:
        pool_size = db_config.get("pool_size", 10)
        max_overflow = db_config.get("max_overflow", 20)
        pool_recycle = db_config.get("pool_recycle", 3600)

        if DB_TYPE == 'oracle':
            db_url = URL.create(
                drivername="oracle+cx_oracle",
                username=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
                query={"service_name": db_config["service_name"]}
            )
        elif DB_TYPE == 'sqlserver':
            # db_url = URL.create(
            #     drivername="mssql+pyodbc",
            #     username=db_config["user"],
            #     password=db_config["password"],
            #     host=f"{db_config['host']}:{db_config['port']}",
            #     database=db_config["database"],
            #     query={
            #         "driver": "ODBC Driver 17 for SQL Server"
            #     }
            # )
            engine = create_engine(
                f'mssql+pyodbc://{db_config["user"]}:{db_config["password"]}'
                f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
                f'?driver=ODBC+Driver+17+for+SQL+Server',
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
                pool_pre_ping=True)



            return engine
        elif DB_TYPE == 'mysql':
            db_url = URL.create(
                drivername="mysql+pymysql",
                username=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"]
            )
        else:
            logger.error(f"Unsupported database type: {DB_TYPE}")
            raise ValueError(f"Unsupported database type: {DB_TYPE}")

        # return create_engine(
        #     f'mssql+pyodbc://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}?driver=ODBC+Driver+17+for+SQL+Server')

        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle
        )
        logger.info("Database engine created successfully.")
        return engine

    except Exception as e:
        print(f"Error creating database engine: {str(e)}")
        logger.exception("Failed to create database engine.")
        raise


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


Base.metadata.create_all(get_engine())
