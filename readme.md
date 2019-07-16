## README

#### To Start
  1. App requires  **config.ini** with api endpoints and algolia credential to be functional.
  2. Install all dependencies by CLI ```pip install -r requirements.txt```
#### Major Update Log
  * 2019-07-09 : Needed updated format of config.ini for environment setup
  * 2019-07-04 : Added logging & log saved to my_app.log
  * 2019-07-03 : Instead of saving index JSON of the last copy as txt file, will be fetching from Algolia instead. 

#### API Module
Handles API requests

#### ETL Module
Handles Tables, DataFrame & Data ETL

#### Test Module
Unit Test for functions by modules
Test Scripts: ```python -m unittest [Test Module]```   
Example: ```python -m unittest tests.test_etl```

#### Environment
env is default to be test unless explicitly passing 'live' as argv[1]   
i.e. ```python main.py live```

#### Scripts
  * Save all dependencies: ```pip freee > requirements.txt```

  

