import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from  S3 to the staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Select and Transform data from staging tables into final tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    cur
    load_staging_tables(cur, conn)
    print('Data successfully Loaded into Staging tables')
    insert_tables(cur, conn)
    print('Data successfully inserted into Database')

    conn.close()
    print('Process successfully terminated')


if __name__ == "__main__":
    main()