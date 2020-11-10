import configparser
import psycopg2
from sql_queries import table_names, file_names, create_table_queries, immigrations_table_copy, load_csv_sql

def drop_tables(cur, conn):
    '''
    Drop any tables in the database if they exist
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: The connection to the database      
    '''
    
    print('Dropping existing tables...')     
    
    for table in table_names:
        query = "DROP table IF EXISTS {}".format(table)
        cur.execute(query)
        conn.commit()
        
    print('All existing tables dropped')
        

def create_tables(cur, conn):
    '''
    Create all the tables in the database
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: The connection to the database      
    '''
    
    print('Creating tables...') 

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

    print('All tables created')

def insert_tables(cur, conn):
    '''
    Load the data from S3 to the datebase
    
    Parameters: 
        cur: The cursor that will be used to execute queries
        conn: The connection to the database      
    '''
    
    print('Inserting data into tables...')

    cur.execute(immigrations_table_copy)
    conn.commit()
    
    for table, file in zip(table_names[1:], file_names[1:]):
        if table == 'demographics':
            delimiter = "';'"
        else: 
            delimiter = "','"

        query = load_csv_sql(table, delimiter, file)

        cur.execute(query)
        conn.commit()

    print('All data inserted into tables')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Database connected")

    drop_tables(cur, conn)
    create_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()