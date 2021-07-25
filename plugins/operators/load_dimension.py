from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table="",
                 sql="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table=table
        self.sql=sql
        self.append_only=append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if not self.append_only:
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info('Inserting into to dimension table {} '.format(self.table))
        redshift.run(getattr(SqlQueries, self.sql).format(self.table))
        self.log.info('Inserting into to dimension table {} completed '.format(self.table))
