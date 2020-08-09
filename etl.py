import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """ Creates staging tables by copying S3 directories

        Returns
        ----------
        cur : psycopg2 connection cursor with type psycopg2.extensions.cursor
        conn: psycopg2 connection with type psycopg2.extensions.connection
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts data from staging tables into fact and dimension tables

        Returns
        ----------
        cur : psycopg2 connection cursor with type psycopg2.extensions.cursor
        conn: psycopg2 connection with type psycopg2.extensions.connection
    """     
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Executes the load_staging_tables and insert_tables functions
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config['CLUSTER']['HOST'],
                                                                                   config['CLUSTER']['DB_NAME'],
                                                                                   config['CLUSTER']['DB_USER'],
                                                                                   config['CLUSTER']['DB_PASSWORD'],
                                                                                   config['CLUSTER']['DB_PORT']))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
