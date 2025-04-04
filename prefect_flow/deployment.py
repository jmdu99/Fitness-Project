from datetime import timedelta

from flow import etl_flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

schedule = IntervalSchedule(interval=timedelta(minutes=10))

deployment = Deployment.build_from_flow(
    flow=etl_flow, name="ETL_Flow_10Min", schedule=schedule, work_queue_name="default"
)

if __name__ == "__main__":
    deployment.apply()
