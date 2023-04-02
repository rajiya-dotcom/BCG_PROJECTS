# About the porject
> Developed an pipeline which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions,     config driven, command line executable through spark-submit) to get the query done by using DataFrame API
> We can run multiple query in a single go for each of the individual component (component is nothing but the class which can have more than 1 function to get the answer of a specific case study)


## Features

- Import the set of a specific case study data from given input data sources
- Process on top of that data to get the answer of the query
- Dump final result as csv tab separator


## How to run
We can run this using :

## spark-submit --driver-memory {driver_memory}g --executor-memory {executor_memory}g main.py config.yaml > log_file.log
