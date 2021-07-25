from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id="",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region= "us-west-2",
                 data_format = "",
                 json_path ="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws = AwsHook(self.aws_conn_id)
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f"DELETE FROM {self.table}")
        self.s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info("Copying data from {} ".format(s3_path))
        redshift.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' \
            SECRET_ACCESS_KEY '{credentials.secret_key}' FORMAT AS JSON '{self.json_path}'")
