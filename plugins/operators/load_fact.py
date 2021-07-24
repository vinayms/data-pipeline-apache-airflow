from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table = "",
                 sql = "",
                 append_only= False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        # Check to append or delete and instart all new.
        if not self.append_only:
            self.log.info("Deleting all entries from {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Inserting data from staging to fact table {} ..".format(self.table))
        insert_sql_query = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(insert_sql_query)
        self.log.info("Inserting to {} table completed".format(self.table))
