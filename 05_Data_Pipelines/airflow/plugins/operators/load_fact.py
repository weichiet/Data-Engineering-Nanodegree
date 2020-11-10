from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                table = "",        
                redshift_conn_id = "",
                create_sql = "",
                insert_sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_sql = create_sql
        self.insert_sql = insert_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        redshift.run(self.create_sql)
        self.log.info(f"Table {self.table} is created")

        insert_sql = f"INSERT INTO {self.table} " + self.insert_sql
        redshift.run(insert_sql)
        self.log.info(f"Data is loaded into table {self.table}")
