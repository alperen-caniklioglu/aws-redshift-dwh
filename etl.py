import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Executes queries in the list copy_table_queries residing in sql_queries in order to load data into database.
    
    Keyword arguments:
    cur  -- cursor object for a given connection
    conn -- connection object which handles the connection to database instance
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Executes queries in the list insert_table_queries residing in sql_queries to populate tables.
    
    Keyword arguments:
    cur  -- cursor object for a given connection
    conn -- connection object which handles the connection to database instance
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads config file to get access parameters to database
    - Connects to database instance
    - Calls load_staging_tables function to load data into database
    - Calls insert_tables function to populate the tables
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()