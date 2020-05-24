from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
     Complete one data quality check: check if returned table is empty
    
    Arguments:
    * redshift_conn_id: connect ID for Redshift
    * dq_checks: A list of sql statements to compare the results with expected value
    
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):
        error_count=0
        failing_tests=[]
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for check in self.dq_checks:
            sql=check.get('check_sql')
            exp_result = check.get('expected_result')
            method=check.get('method')
            
            records = redshift_hook.get_records(sql)[0]

         
            # check greater than method
            if method=='greater_than':
                if exp_result <= records[0]:
                    error_count += 1
                    failing_tests.append(sql)
            # check equal method
            elif method=='equal':
                if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)
            
            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            
            else:
                self.log.info(f"Data quality check passed: sql: {sql}, method: {method}, result: {exp_result}")
