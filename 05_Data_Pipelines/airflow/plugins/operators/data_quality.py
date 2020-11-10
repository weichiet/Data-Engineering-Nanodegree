from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,     
                redshift_conn_id = "",
                test_case_sqls = "",
                expected_results = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_case_sqls = test_case_sqls
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for (sql, result) in zip(self.test_case_sqls, self.expected_results):
            record = redshift.get_records(sql)
            
            if record[0][0] != result:
                raise ValueError(f"Data quality check failed.")
                
        self.log.info(f"Data quality check passed.")
