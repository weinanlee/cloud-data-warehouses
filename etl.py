import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the song and log data in the staging table.
    
    Keyword arguments:
    cur  -- cursor
    conn -- connection to database
    
    """
    for query in copy_table_queries:
        print("Executing: ", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Extract the data from staging table and insert them into tables.
    
    Keyword arguments:
    cur  -- cursor
    conn -- connection to database
    
    """
    for query in insert_table_queries:
        print("Executing: ", query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()