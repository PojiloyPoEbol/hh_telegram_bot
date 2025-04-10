import datetime
import pandas as pd
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import *
import psycopg2 as ps



class AbstractDbAPI:
    @staticmethod
    def create_conn(passlog_path,ip_or_localhost,con_port,scheme):
        with open(f"{passlog_path}", "r") as file:
            passlog = file.readline()
        engine = create_engine(f"postgresql+psycopg2://{passlog}@{ip_or_localhost}:{con_port}/{scheme}")
        return engine

    @staticmethod
    def create_from_df_table(engine, df,schema,table):
        try:
            df.to_sql(f'{table}',schema=schema ,con=engine, index=False)
            print(f"Таблица {table} успешно создана")
        except SQLAlchemyError as e:
            print(f"Ошибка при создании таблицы: {e}")

    @staticmethod
    def delete_from_table_cond(engine,table,condition):
        Session = sessionmaker(bind=engine)
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'DELETE FROM {table} WHERE {condition};'))
                session.commit()
                print(f'Из таблицы {table} удалены значения по условию:{condition}')
        except SQLAlchemyError as e:
            print(f'Ошибка при выполнении запроса {e}')


    @staticmethod
    def delete_from_table_col(engine, table, column):
        Session = sessionmaker(bind=engine)
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'ALTER TABLE {table} DROP COLUMN {column};'))
                session.commit()
                print(f'Столбец {column} успешно удален из таблицы {table}')
        except SQLAlchemyError as e:
            print(f"Ошибка при удалении данных: {e}")


    @staticmethod
    def truncate_table(engine,table):
        Session = sessionmaker(bind=engine)
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'TRUNCATE TABLE {table}'))
                session.commit()
            print(f"Таблица {table} успешно очищена")
        except SQLAlchemyError as e:
            print(f"Ошибка при очистке таблицы: {e}")

    @staticmethod
    def drop_table(engine,table):
        Session = sessionmaker(bind=engine)
        try:
            with Session(bind=engine) as session:
                session.execute(text(f'DROP TABLE {table}'))
                session.commit()
                print(f'Таблица {table} успешно удалена!')
        except SQLAlchemyError as e:
            print(f'Ошибка при удалении таблицы: {e}')

    @staticmethod
    def read_sql(engine,query):
        try:
            df = pd.read_sql(query,engine)
            return df
        except SQLAlchemyError as e:
            print(f"Ошибка при чтении данных: {e}")


    @staticmethod
    def insert_sql(engine, table, df, options):
        try:
            if options == 'new_table':
                df.to_sql(table, con=engine, index=False)
            elif options == 'append':
                df.to_sql(table, con=engine, index=False, if_exists='append')
            elif options == 'replace':
                df.to_sql(table, con=engine, index=False, if_exists='replace')
            print("Данные успешно записаны")
        except SQLAlchemyError as e:
            print(f"Ошибка при записи данных: {e}")


    @staticmethod
    def execute(engine, query):
        Session = sessionmaker(bind=engine)
        try:
            with Session(bind=engine) as session:
                session.execute(text(query))
                session.commit()
            print("Запрос успешно выполнен")
        except SQLAlchemyError as e:
            print(f"Ошибка при выполнении запроса: {e}")

