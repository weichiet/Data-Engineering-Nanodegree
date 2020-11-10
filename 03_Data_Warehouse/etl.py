import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load data from S3 into staging tables
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: Connection to the database
    '''         
    
    print('Loading staging tables...')
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

    print('Staging tables loaded')
        
def insert_tables(cur, conn):
    '''
    Load data from staging tables to analytics tables
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: Connection to the database
    '''  
    
    print('Inserted data into tables...')
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    print('All data inserted into tables')

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