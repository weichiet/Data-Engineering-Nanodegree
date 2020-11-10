from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                table = "",        
                redshift_conn_id = "",
                create_sql = "",
                insert_sql = "",
                insert_type = "append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.insert_type = insert_type

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift.run(self.create_sql)
        self.log.info(f"Table {self.table} is created")

        if self.insert_type == 'delete':
            self.log.info(f"Clearing data from {self.table} table")            
            redshift.run(f"DELETE FROM {self.table}")

        insert_sql = f"INSERT INTO {self.table} " + self.insert_sql
        redshift.run(insert_sql)
        self.log.info(f"Data is loaded into {self.table} table")

