#!/usr/bin/env bash
#chmod +x entrypoint.sh

# initialize the database
airflow initdb

sleep 10

# run the airflow scheduler as 
exec airflow scheduler -D

sleep 2

exec airflow webserver -p 8080 -D

