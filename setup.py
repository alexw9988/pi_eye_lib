
from setuptools import setup, find_packages

setup(
    name='pi_eye_lib',
    version='0.1',
    packages=find_packages(include=['pi_eye_lib', 'pi_eye_lib.*']),
    install_requires=[
        'minio',
        'celery',
        'psycopg2',
        'SQLAlchemy',
        'importlib_metadata'
    ]
)
