from setuptools import setup

setup(name='celery-task-locker',
      version='0.1.3',
      description='Ensuring a task is only executed one at a time',
      url='https://github.com/maxtwen/celery-task-locker',
      author='maxtwen',
      author_email='maxtwen@yandex.ru',
      license='MIT',
      packages=['celery_task_locker'],
      zip_safe=False,
      install_requires=[
            'celery>=3.1.23',
            'redis>=2.10.5'
      ])