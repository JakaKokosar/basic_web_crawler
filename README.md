Crawler dependencies
--------------------


Make sure docker is installed on your system:

    https://docs.docker.com/install/

Python packages:

    pip install -r requirements.txt
    


Database
--------

Run PostgresDB instance locally and then run provided SQL script (crawldb.sql):

    docker run --rm --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 postgres
    

Run
----
    cd crawler
    python web_crawler.py 8

