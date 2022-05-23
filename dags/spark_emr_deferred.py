from datetime import datetime
from airflow import DAG, AirflowException
from airflow.triggers.base import BaseTrigger
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models import TaskInstance
from typing import Any, Dict, Tuple
import asyncio
import logging
import os
import uuid


os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["AWS_SESSION_TOKEN"] = ""
os.environ["AWS_DEFAULT_REGION"] = "af-south-1"
global_args = {
    "spark-step-name": "calculate_pi_emr_deferred",
    "spark-class": "org.apache.spark.examples.SparkPi",
    "spark-jar": "spark-examples_2.11-2.4.3.jar",
    "spark-jar-args": ["3000"],
    "cluster-id": "j-XXXXXXXXXXXXX",
}

class EmrSensorTrigger(BaseTrigger):
    def __init__(self, job_uuid: str, step_id: str, cluster_id: str) -> None:
        super().__init__()
        self.job_uuid = job_uuid
        self.step_id = step_id
        self.cluster_id = cluster_id
        self.step_checker = EmrStepSensor(
                task_id=f'watch_step',
                job_flow_id=cluster_id,
                step_id=step_id,
            )
        
    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return ("spark_emr_deferred.EmrSensorTrigger", {"job_uuid": self.job_uuid, "step_id": self.step_id, "cluster_id": self.cluster_id})

    async def run(self):
        # TODO: await emr response
        try:
            state = ""
            while state not in self.step_checker.target_states + self.step_checker.failed_states:
                await asyncio.sleep(10)
                response = self.step_checker.get_emr_response()
                state = self.step_checker.state_from_response(response)
                logging.info(f"State for step {self.step_id} - {state}")

            if state in self.step_checker.target_states:
                yield TriggerEvent(True)
            else:
                yield TriggerEvent(False)
        except Exception as e:
            logging.warn(str(e))
            yield TriggerEvent(False)


class EmrDeferredOperator(EmrAddStepsOperator):
    def __init__(self, *args, **kwargs) -> None:
        sparkSteps = self.getSparkSteps(*args, **kwargs)
        self.cluster_id = global_args['cluster-id']
        super().__init__(steps=sparkSteps, job_flow_id=self.cluster_id, *args, **kwargs)
        self.uuid = uuid.uuid4()

    def execute(self, context):
        # TODO: Handle scheduler restarts.
        return_value = super().execute(context)
        step_id = return_value[0]
        self.defer(trigger=EmrSensorTrigger(job_uuid=self.uuid, step_id=step_id, cluster_id=self.cluster_id), method_name="onSparkJobFinish")
    
    def onSparkJobFinish(self, context, event=None):
        if not event:
            raise AirflowException('Spark job failed')
        return event

    def getSparkSteps(self, *args, **kwargs):
        sparkStepName = global_args['spark-step-name']
        sparkClass = global_args['spark-class']
        sparkJar = global_args['spark-jar']
        sparkJarArgs = global_args['spark-jar-args']

        sparkSteps = [
            {
                'Name': sparkStepName,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--deploy-mode', 'cluster',
                    '--class', sparkClass,
                    '--master', 'yarn', 
                    sparkJar] + sparkJarArgs,
                },
            }
        ]
        return sparkSteps


dag = DAG('spark_emr_deferred',
          description='Spark EMR Deferred',
          schedule_interval=None,
          max_active_runs=1,
          start_date=datetime(2022, 3, 20), catchup=False)

step_adder = EmrDeferredOperator(
    task_id='add_steps',
    dag=dag,
)

step_adder

