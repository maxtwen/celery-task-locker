# coding: utf-8

import redis


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

