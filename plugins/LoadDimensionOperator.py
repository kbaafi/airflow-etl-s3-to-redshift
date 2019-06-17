from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                conn_id = '',
                sql_stmt = '',
                target_table = '',
                truncate_before_insert = True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.connection_id = conn_id
        self.fact_select_statement = sql_stmt
        self.target_table = target_table
        self.truncate_before_insert = truncate_before_insert

    def execute(self, context):
        pg_hook = PostgresHook(self.connection_id)
        insert_statement = f"""insert into {self.target_table} 
                            {self.fact_select_statement}"""

        if self.truncate_before_insert:
            delete_stmt = f"delete from {self.target_table}"
            pg_hook.run(delete_stmt)

        pg_hook.run(insert_statement)

class LoadDimensionPlugin(AirflowPlugin):
    name = "LoadDimension"
    operators = [LoadDimensionOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
