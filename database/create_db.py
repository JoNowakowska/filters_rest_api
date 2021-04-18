"""
Create database if not exists and 5 tables based on 5 sql files.
Add data to 3 of them (PROGRAMY, OSIE, DZIALANNIA) from 3 csv files (programy, osie, dzialania).
"""

import csv
import sqlite3
from typing import List


DB_PATH = "programy_w_polsce.db"

SQL_FILES = [
    r'database\PROGRAMY.sql',
    r'database\OSIE.sql',
    r'database\DZIALANIA.sql',
    r'database\FTD.sql',
    r'database\FTD_ELEMENTY.sql'
]

SQL_INSERT_TO_PROGRAMY = 'INSERT INTO PROGRAMY (id_program, nazwa) VALUES (?, ?);'
SQL_INSERT_TO_OSIE = 'INSERT INTO OSIE (id_program, id_os, nazwa) VALUES (?, ?, ?);'
SQL_INSERT_TO_DZIALANIA = 'INSERT INTO DZIALANIA (id_program, id_os, id_dzl, nazwa) VALUES (?, ?, ?, ?);'


def create_connection(db_path: str):
    """Create a connection to the sqlite db programy_w_polsce.db"""

    try:
        con = sqlite3.connect(db_path)
    except Exception as e:
        print(e)
        con = None

    return con


def create_tables_in_db(sql_files: List) -> None:
    """create tables in the db"""

    con = create_connection(DB_PATH)

    if con is not None:
        try:
            c = con.cursor()
            for file in sql_files:
                with open(file) as of:
                    f_str = of.read()
                    c.executescript(f_str)

            con.commit()
            con.close()

        except Exception as e:
            print(e)


def add_data_to_a_table(file, sql_query: str) -> None:
    """read a csv file and add data to programy table"""

    con = create_connection(DB_PATH)

    if con is not None:
        with open(file, encoding="utf8") as f:
            rows = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            header = next(rows)

            list_of_rows = [row[0].split(',') for row in rows]

            for row in rows:
                print(row)

            c = con.cursor()
            c.executemany(sql_query, list_of_rows)

        con.commit()
        con.close()


if __name__ == '__main__':
    create_tables_in_db(SQL_FILES)
    add_data_to_a_table(r'database\programy.csv', SQL_INSERT_TO_PROGRAMY)
    add_data_to_a_table(r'database\osie.csv', SQL_INSERT_TO_OSIE)
    add_data_to_a_table(r'database\dzialania.csv', SQL_INSERT_TO_DZIALANIA)

