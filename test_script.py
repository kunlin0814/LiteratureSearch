import datetime
from prefect import flow, task


def generate_task_name():
    date = datetime.datetime.now(datetime.timezone.utc)
    return f"{date:%A}-is-a-lovely-day"


@task(name="My Example Task",
      description="An example task for the docs.",
      task_run_name=generate_task_name)
def my_task(name):
    pass


@flow
def my_flow():
    # creates a run with a name like "Thursday-is-a-lovely-day"
    my_task(name="marvin")


if __name__ == "__main__":
    my_flow()