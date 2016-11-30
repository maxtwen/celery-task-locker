# coding: utf-8

import redis
from trollius import Task
from celery.task import task
from celery.canvas import group


class TaskLocker(object):

    def __init__(self, task_name, redis_db, redis_host, redis_port):

        self.r = redis.StrictRedis(db=redis_db, host=redis_host, port=redis_port)
        self.task_name = task_name

    @staticmethod
    def get_key(*args):
        return ''.join(map(str, args))

    def check_lock(self, id_=0):

        return self.r.get(TaskLocker.get_key(self.task_name, id_))

    def lock(self, id_=0):

        self.r.set(TaskLocker.get_key(self.task_name, id_), 'true')

    def unlock(self, id_=0):

        self.r.delete(TaskLocker.get_key(self.task_name, id_))

    def check_or_lock(self):
        if self.check_lock():
            return True
        else:
            self.lock()
            return False


class LockedTask(Task):
    abstract = True

    @staticmethod
    def unlock(task_name, task_id):
        locker = TaskLocker(task_name)
        locker.unlock(task_id)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.unlock(self.name, args[0])

    def on_success(self, retval, task_id, args, kwargs):
        self.unlock(self.name, args[0])


def callback_unlock(queue):
    @task(queue=queue)
    def func(task, id_):
        locker = TaskLocker(task)
        return locker.unlock(id_)
    return func


def locked_task(f):
    def dec(*args, **kwargs):
        ts = TaskLocker(args[0].name)  # args[0] - self
        if ts.check_or_lock():
            return 'Locked'
        try:
            task_result = f(*args, **kwargs)
        except:
            raise
        else:
            return task_result
        finally:
            ts.unlock()

    return dec


def locked_group(task, ids, max_count=None):
    locker = TaskLocker(task.name)
    return group([task.si(id_) for id_ in locker.get_unprocessed_tasks(ids, max_count=max_count)])