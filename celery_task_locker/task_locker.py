# coding: utf-8

__all__ = ['locked_task', 'locked_group']

import redis
from celery.canvas import group
from celery.task import task
from celery import Task
from celery import current_app

REDIS_HOST = current_app.conf.TL_REDIS_HOST
REDIS_PORT = current_app.conf.TL_REDIS_PORT
REDIS_DB = current_app.conf.TL_REDIS_DB


class TaskLocker(object):
    """Class for lock the task by means of redis
    """
    def __init__(self, task_name):
        """
        :param task_name: task unique name
        """
        self.r = redis.StrictRedis(db=REDIS_DB, host=REDIS_HOST, port=REDIS_PORT)
        self.task_name = task_name

    @staticmethod
    def get_key(*args):
        """generate key for redis
        """
        return ''.join(map(str, args))

    def check_lock(self, id_=0):
        """checks for the presence of the task id to lock it

        :param id_: task id
        :return: redis server response
        """
        return self.r.get(TaskLocker.get_key(self.task_name, id_))

    def lock(self, id_=0):
        """lock task by task id

        :param id_: task id
        """
        self.r.set(TaskLocker.get_key(self.task_name, id_), 'true')

    def unlock(self, id_=0):
        """unlocks the task of its identifier

        :param id_: task id
        """
        self.r.delete(TaskLocker.get_key(self.task_name, id_))

    def get_unprocessed_tasks(self, ids, max_count=None):
        """
        :param ids: sequence of task id's
        """
        for id_ in ids:
            if max_count and len(self.r.keys('%s*' % self.task_name)) >= max_count:
                raise StopIteration
            if self.check_lock(id_):
                continue
            self.lock(id_)
            yield id_

    def check_or_lock(self):
        if self.check_lock():
            return True
        else:
            self.lock()
            return False


def callback_unlock(queue):
    @task(queue=queue)
    def func(task, id_):
        """callback-function, delete trash key from redis

        :param task: celery task
        :param id_: task id
        :return: redis server response
        """
        locker = TaskLocker(task)
        return locker.unlock(id_)
    return func


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


def locked_group(task, ids, max_count=None):
    """generates a group locked tasks, after successful completion of each task will be invoked
     callback, which will remove the key used to lock

    :param task: celery task
    :param ids: sequence of task id's
    """
    locker = TaskLocker(task.name)
    return group([task.si(id_) for id_ in locker.get_unprocessed_tasks(ids, max_count=max_count)])


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

