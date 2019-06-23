from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                conn_id = '',
                sql_stmt = '',
                target_table = '',
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.connection_id = conn_id
        self.fact_select_statement = sql_stmt
        self.target_table = target_table

    def execute(self, context):
        pg_hook = PostgresHook(self.connection_id)
        insert_statement = f"""insert into {self.target_table} {self.fact_select_statement}"""

        pg_hook.run(insert_statement)
