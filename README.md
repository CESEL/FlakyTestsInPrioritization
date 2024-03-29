# Replication Package
Paper Title: "The Impact of Flaky Tests on Historical Test Prioritization on Chrome"

Authors: Emad Fallahzadeh, and Peter C. Rigby

Contact us: emad.fallahzadeh@concordia.ca, peter.rigby@concordia.ca

## Getting Started
The following instructions help you to get a copy of project up and running.

### Prerequisites
To run this project you need to install the following:
* Python 3.7 or higher
* PostgreSQL 10.18 or higher

### Installation
1. Download data from https://doi.org/10.5281/zenodo.5576626
2. Unzip compressed files by the following command in terminal:
> cat x*.gz.part | tar -x -vz -f -
3. Execute following command to create the chromium database in terminal:
> createdb chromium
4. To import test table run the following:
> psql -U username -d database -1 -f chromium_dump.sql


### Usage
1. Run the following commands to prepare tables regarding blocking and non-blocking flaky failures scenarios:

blocking:
> psql chromium -f convert_chromium.sql

non-blocking:
> psql chromium -f convert_chromium_unexpected.sql

2. In the following scripts replace ‘secret’ in the psycopg2.connect() with database password you set.
3. To install dependency packages:
> pip install -r requirements.txt
4. To remove repeated tests in each build run:
> python3 RemoveRepeatedTestsInEachBuild.py -t tests

> python3 RemoveRepeatedTestsInEachBuild.py -t tests_unexpected
5. Run the following commands to get the results from the algorithms:

No-Prioritization:
> python3 fifo.py

Kim and Porter:
> python3 kimporter.py

Elbaum:
> python3 elbaum.py
