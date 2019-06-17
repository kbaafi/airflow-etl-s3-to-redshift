from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

class S3ToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)

    copy_sql = """
        copy {} 
        from '{}' 
        access_key_id '{}' secret_access_key '{}'
        {}"""
    
    @apply_defaults
    def __init__(self,
        redshift_conn_id = "",
        aws_conn_id = "",
        table = "",
        s3_bucket = "",
        s3_key = "",
        copy_params = [],
        overwrite = False,
        *args,
        **kwargs):

        super(S3ToRedshiftOperator,self).__init__(*args,**kwargs)

        self.redshift_conn_id       = redshift_conn_id
        self.aws_conn_id            = aws_conn_id
        self.table                  = table
        self.s3_bucket              = s3_bucket
        self.s3_key                 = s3_key
        self.copy_params            = copy_params
        self.overwrite              = overwrite


    def execute(self, context): 
        aws_hook            = AwsHook(self.aws_conn_id)
        aws_credentials     = aws_hook.get_credentials()
        redshift_hook       = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.overwrite == True:
            self.log.info(f"Clearing data from destination table {self.table}")
            redshift_hook.run(f"delete from {self.table}")

        self.log.info("Copying data from S3 to Redshift")

        copy_options = ("\n").join (self.copy_params)
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            copy_options
        )

        redshift_hook.run(formatted_sql)


class S3ToRedshift(AirflowPlugin):
    name = "S3ToRedshift"
    operators = [
        S3ToRedshiftOperator
        ]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
