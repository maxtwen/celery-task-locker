# celery-task-locker
Ensuring a task is only executed one at a time

### Installing
pip install celery-task-locker

## Code Example

```python
from celery.task import PeriodicTask
from datetime import timedelta
from celery import Celery

from celery_task_locker import locked_task, locked_group


app = Celery()

@app.task()
def some_task(task_id):
    print("task with task id %s will run one time" % task_id)

class PeriodicTask(PeriodicTask):
    run_every = timedelta(minutes=10)

    @locked_task
    def run(self, **kwargs):
        job = locked_group(some_task, [1,2,3,4])
        job.apply_async()
```