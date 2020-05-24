from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    
    """
    subdag to load data from Redshift fact table
    
    Arguments:
    * redshift_conn_id: connect ID for Redshift
    * table: table name in Reshift
    * sql_query: name of query to be called from `SqlQueries` class
    * append_data: Whether data will be append to exsiting table, or delete exsiting and create new. Default False
    
    """
    ui_color = '#F98866'
    query = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.append_data=append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data==False:
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info('Clearing data from destination Redshift table {}'.format(self.table))
        formatted_sql = LoadFactOperator.query.format(
            self.table,
            getattr(SqlQueries,self.sql_query)
        ) 
        self.log.info("Load data from staging tables into fact table {}".format(self.table))
        redshift.run(formatted_sql)
        
        