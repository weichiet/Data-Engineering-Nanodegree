from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_json = """
        COPY {}
        FROM '{}'
        FORMAT AS {} '{}'
        TIMEFORMAT 'epochmillisecs'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    copy_csv = """
        COPY {}
        FROM '{}'
        FORMAT AS {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 table = "",        
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 s3_bucket = "",
                 s3_key = "",
                 create_sql = "",
                 data_format = 'csv',
                 json_path = '',
                 delimiter = ",",
                 ignore_headers = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.create_sql = create_sql
        self.data_format = data_format
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        redshift.run(self.create_sql)
        self.log.info(f"Table {self.table} is created")

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        if self.json_path.endswith('json'):
            self.json_path = f"s3://{self.s3_bucket}/{self.json_path}"

        if self.data_format == 'csv':
            formatted_sql = StageToRedshiftOperator.copy_csv.format(
                self.table,
                s3_path,
                self.data_format,                 
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )           
            
        elif self.data_format == 'json':            
            formatted_sql = StageToRedshiftOperator.copy_json.format(
                self.table,
                s3_path,
                self.data_format, 
                self.json_path,
                credentials.access_key,
                credentials.secret_key
            )
            
        else:
            raise ValueError(f"Invalid S3 data format: {self.data_format}")
        
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)
        self.log.info(f"Table {self.table} is loaded to Redshift")
