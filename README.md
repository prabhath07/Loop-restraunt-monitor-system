The Project is Built using Python Fast API.
The Data given in the problem statement has been taken as raw files and the data is inserted into the data base.
The code for the data push can be found in the file: db_population.py

The fast api implementation for the problem is located at the file : main.py

The main loggic behind calculation of the output metrics is explained in the logic.txt

Setup Process:
Install the dependencies listed in requirements.txt
After changing the DB link assigned in the file , run python db_population.py
The Data will then get populated into the DB.

After that Run uvicorn main:app to start the fast api server.

The Swagger documentation of the Routes offered can be accessed at localhost:8000/docs.
