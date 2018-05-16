from setuptools import setup

setup(
    name="dataflow_pipeline_dependencies",
    version="1.0.0",
    author="Nahuel Lofeudo",
    author_email="nlofeudo@google.com",
    description=("Custom python utils needed for dataflow cloud runner"),
    packages=[
        'utils'
    ]
)