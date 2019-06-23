from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 test_cases=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.qa_test_cases = test_cases

    def execute(self, context):
        pg_hook = PostgresHook(self.conn_id)
        
        for test_case in self.qa_test_cases:
            sql = test_case['sql']
            count_threshold = test_case['count_threshold']
            pass_message = test_case['pass_message']
            fail_message = test_case['fail_message']

            records = pg_hook.get_records(sql)

            if len(records) < count_threshold+1 or len(records[0]) < count_threshold+1:
                logging.error(fail_message)
                raise ValueError(fail_message)

            num_records = records[0][0]

            if num_records < count_threshold+1:
                logging.error(fail_message)
                raise ValueError(fail_message)

            logging.info(pass_message.format(num_records))