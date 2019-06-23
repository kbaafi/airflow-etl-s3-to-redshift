import logging
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class HasRowsOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        connection_id = "",
        table = "",
        *args,
        **kwargs):
        
        super(HasRowsOperator,self).__init__(*args,**kwargs)
        self.table = table
        self.connection_id = connection_id

    
    def execute(self,context):
        pg_hook = PostgresHook(self.connection_id)
        records = pg_hook.get_records(f"select count(*) from {self.table}")

        if (len(records)<1 or len(records[0])<1):
            logging.error(f"Data quality check failed for table{self.table}. No records found")
            raise ValueError(f"Data quality check failed for table{self.table}. No records found")

        num_records = records[0][0]

        if num_records < 1:
            logging.error(f"Data quality check failed for table{self.table}. No records found")
            raise ValueError(f"Data quality check failed for table{self.table}. No records found")

        logging.info(f"Data quality checks on table {self.table} passed with {num_records} records")
