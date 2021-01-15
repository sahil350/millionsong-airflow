from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tests = [],
                 table='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        self.table = table

    def execute(self, context):
        """
        This method performs the data quality checks to ensure intgerity
        and logs the appropriate message. It raises a ValueError in case
        the quality check fails.

        * INPUTS
            redshift_conn_id
                Airflow Connection Id to Redshift
            tests
                A list of dictionary with keys `sql`, and `expected_result`
                `sql` is the test SQL query and `expected_result` is the 
                expected result from the execution of `sql`
            table
                The name of the table
        """
        errors = []
        self.log.info('Checking data quality')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in self.tests:
            sql_stmt = test.get('sql')
            result = test.get('expected_result')
            records = redshift.get_records(sql_stmt)
            if len(records) < 1 or len(records[0]) < 1 or result != records[0]:
                errors.append(sql_stmt)
                
        if len(errors) > 0:
            self.log.info("Checks failed")
            self.log.info(errors)
            raise ValueError("Data quality checks failed")
        
        self.log.info("Data quality checks succeeded")
