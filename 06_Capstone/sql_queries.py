import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
  
# CREATE TABLES
immigrations_table_create = ("""
    CREATE TABLE IF NOT EXISTS immigrations (
        cicid BIGINT NOT NULL,
        year INT,
        month INT,
        city INT,
        country INT,
        port VARCHAR,
        arrival_date DATE,
        arrival_mode INT,
        state VARCHAR,
        departure_date DATE,
        age INT,
        visa_code INT,
        count INT,
        date VARCHAR,
        visa_post VARCHAR,        
        occupation VARCHAR,
        arrival_flag VARCHAR,
        departure_flag VARCHAR,
        update_flag VARCHAR,
        match_flag VARCHAR,
        birth_year INT,
        date_allowed_to DATE,
        gender VARCHAR,
        ins_number VARCHAR,
        airline VARCHAR,
        admission_number FLOAT,
        flight_number VARCHAR,
        visa_type VARCHAR,
        PRIMARY KEY(cicid),
        FOREIGN KEY (city) REFERENCES countries(country_id),
        FOREIGN KEY (country) REFERENCES countries(country_id),
        FOREIGN KEY (port) REFERENCES ports(port_code),
        FOREIGN KEY (arrival_mode) REFERENCES arrival_mode(arrival_code),
        FOREIGN KEY (state) REFERENCES states(state_code),
        FOREIGN KEY (visa_code) REFERENCES visa_type(visa_code)
    );""")

arrival_mode_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        arrival_mode ( 
            arrival_code INTEGER,
            arrival_mode VARCHAR,
            PRIMARY KEY(arrival_code)
        );""")

countries_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        countries ( 
            country_id INTEGER,
            country_name VARCHAR,
            PRIMARY KEY(country_id)
        );""")

ports_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        ports ( 
            port_code VARCHAR,
            port_name VARCHAR,
            PRIMARY KEY(port_code)
        );""")

states_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        states ( 
            state_code VARCHAR,
            state_name VARCHAR,
            PRIMARY KEY(state_code)
        );""")

visa_type_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        visa_type ( 
            visa_code INT,
            visa_type VARCHAR,
            PRIMARY KEY(visa_code)
        );""")

demographics_table_create = ("""
        CREATE TABLE IF NOT EXISTS demographics (
            city VARCHAR,
            state VARCHAR,
            median_age FLOAT,
            male_population INT,
            female_population INT,
            total_population INT,
            number_of_veterans INT,
            foreign_born INT,
            average_household_size FLOAT,
            state_code VARCHAR,
            race VARCHAR,
            count INT,
            PRIMARY KEY(city)
        );
        """)

airport_codes_table_create = ("""
            CREATE TABLE IF NOT EXISTS airport_codes (
                ident VARCHAR,
                type VARCHAR,
                name VARCHAR,
                elevation_ft FLOAT,
                continent VARCHAR,
                iso_country VARCHAR,
                iso_region VARCHAR,
                municipality VARCHAR,
                gps_code VARCHAR,
                iata_code VARCHAR,
                local_code VARCHAR, 
                coordinates VARCHAR,
                PRIMARY KEY(ident)
                );
           """)
  
# COPY TABLES

ARN = config.get("IAM_ROLE", "ARN")
S3_BUCKET = config.get('S3', 'S3_BUCKET')
ARRIVAL_MODE = config.get('S3', 'ARRIVAL_MODE')
IMMIGRATIONS = config.get('S3', 'IMMIGRATIONS')
VISA_TYPE = config.get('S3', 'VISA_TYPE')
COUNTRIES = config.get('S3', 'COUNTRIES')
PORTS = config.get('S3', 'PORTS')
STATES = config.get('S3', 'STATES')
AIRPORT_CODES = config.get('S3', 'AIRPORT_CODES')
DEMOGRAPHICS = config.get('S3', 'DEMOGRAPHICS')

immigrations_table_copy = ("""copy immigrations 
                                from 's3://{}/{}/'
                                format as parquet
                                credentials 'aws_iam_role={}';""").format(S3_BUCKET, IMMIGRATIONS, ARN)

def load_csv_sql(table_name, delimiter, csv_file):
    '''
    Generate SQL query to copy data in csv format to datebase
    
    Parameters: 
        table_name: Name of the table
        delimiter: Delimiter of the csv 
        csv_file: Name of the csv file
    '''
    
    table_copy_sql = ("""copy {} 
                            from 's3://{}/{}'
                            csv
                            ignoreheader as 1
                            delimiter {}
                            compupdate off 
                            region 'us-west-2'
                            credentials 'aws_iam_role={}';""").format(table_name, S3_BUCKET, csv_file, delimiter, ARN)
    return table_copy_sql


# QUERY AND NAME LISTS

table_names = ['immigrations', 'arrival_mode', 'countries', 'ports', 'states', 'visa_type', 'airport_codes', 'demographics']
file_names =  [IMMIGRATIONS, ARRIVAL_MODE, COUNTRIES, PORTS, STATES, VISA_TYPE, AIRPORT_CODES, DEMOGRAPHICS]
create_table_queries = [arrival_mode_table_create, countries_table_create, ports_table_create, states_table_create, \
                           visa_type_table_create, immigrations_table_create, airport_codes_table_create, demographics_table_create]
