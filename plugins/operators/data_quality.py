from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_stmt='',
                 result=None,
                 table='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.result = result
        self.table = table

    def execute(self, context):
        """
        This method performs the data quality checks to ensure intgerity
        and logs the appropriate message. It raises a ValueError in case
        the quality check fails.

        * INPUTS
            redshift_conn_id
                Airflow Connection Id to Redshift
            sql_stmt
                The SQL statement to execute for checking data quality
            result
                The expected result from the execution of SQL
            table
                The name of the table
        """
        self.log.info('Checking data quality')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(self.sql_stmt)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Data quality check failed. {self.table} returned no results')
        if self.result != records[0]:
            raise ValueError(f'Data quality check failed. {self.result} != {records[0]}')
        self.log.info('Data quality check succeeded')
