from setuptools import setup, find_packages

setup(
    name="etl-pipeline",
    version="1.0.0",
    description="Multithreaded ETL Pipeline for CSV, JSON, and SQLite",
    author="Your Name",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests*", "examples*"]),
    extras_require={
        "dev": ["pytest>=7.0", "pytest-cov>=4.0"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
)
