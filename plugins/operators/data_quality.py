from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
import operator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_check_sql="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_check_sql=dq_check_sql

    def execute(self, context):
        ops = {'>': operator.gt,
           '<': operator.lt,
           '>=': operator.ge,
           '<=': operator.le,
           '==': operator.eq}
        if len(self.dq_check_sql) <=0:
            self.log.info("No quality check SQL statments so just returning .. ")
            return
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        failed_dq_check_sql = []
        #For each dq_check_sql execute the statements.
        for check in self.dq_check_sql:
            sql = check.get('test_sql')
            expected_result = check.get('expected_result')
            comparison = check.get('comparison')
            
            try:
                self.log.info("Running data quality check SQL : {}".format(sql))
                records = redshift_hook.get_records(sql)[0]
            except Exception as e:
                self.log.info("Data quality check failed with exception : {} ".format(e))
            
            if ops[comparison](records[0], expected_result):
                self.log.info("Data quality check passed")
            else:
                self.log.info("Data quality check failed did not match the expected result")
                failed_dq_check_sql.append(sql)
                
        if len(failed_dq_check_sql) > 0:
            self.log.info("Data quality check failed for {} tests".format(len(failed_dq_check_sql)))
            raise ValueError("Data quaility check failed!!!")              
        else:
            self.log.info("All data quailty tests passed!!!")
        
        