from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 schema='',
                 table='',
                 sql_stmt='',
                 truncate='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f'Loading {self.table} table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            sql = f"TRUNCATE table {self.table}"
            redshift.run(sql)
        formatted_sql = f"INSERT INTO {self.schema}.{self.table} ({self.sql_stmt})"
        redshift.run(formatted_sql)
        self.log.info(f'Success: loaded {self.table} table')
