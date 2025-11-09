from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    description="CLI-based background job queue system",
    author="Niranjan",
    packages=find_packages(),
    install_requires=[
        "click>=8.1.0",
        "python-dateutil>=2.8.2",
    ],
    entry_points={
        "console_scripts": [
            "queuectl=queuectl.cli:main",
        ],
    },
    python_requires=">=3.7",
)

