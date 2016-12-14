**Usage examples:**

1) As simple task decorator:

        @locked_task
        def run(self, **kwargs):
            pass
        

2) Group of locked tasks


        @app.task()
        def some_task(task_id):
            pass
    
        
        class PeriodicTask(PeriodicTask):
            run_every = timedelta(minutes=10)
        
            @locked_task
            def run(self, **kwargs):
                job = locked_group(some_task, [1,2,3,4])
                job.apply_async()
