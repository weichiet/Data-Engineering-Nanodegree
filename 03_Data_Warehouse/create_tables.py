import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop drops any tables in the database if they exist
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: The connection to the database
    '''      
    
    print('Dropping existing tables...')     
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    print('All existing dropped')
        

def create_tables(cur, conn):
    '''
    Create the staging, fact and dimension tables
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: Connection to the database
    '''      
    
    print('Creating tables...')    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

    print('All tables created')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Database connected")

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()