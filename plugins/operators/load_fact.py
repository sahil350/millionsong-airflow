from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 schema='',
                 table='',
                 sql_stmt='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info("Loading songplays table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = f"INSERT INTO {self.schema}.{self.table} ({self.sql_stmt})"
        redshift.run(formatted_sql)
        self.log.info("Success: loaded songplays table")
