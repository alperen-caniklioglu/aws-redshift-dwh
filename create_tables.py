import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Drops tables using queries in the list "drop_table_queries" defined in sql_queries.
    
    Keyword arguments:
    cur  -- cursor object for a given connection
    conn -- connection object which handles the connection to database instance
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - Creates tables using queries in the "create_table_queries" defined ins sql_queries.
    
    Keyword arguments:
    cur  -- cursor object for a given connection
    conn -- connection object which handles the connection to database instance
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads property file which contains access properties
    - Connects to database
    - Calls function "dop_tables" to drop tables if they already exist. 
    - Calls function "create_tables" to create tables if they do not exist yet. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()