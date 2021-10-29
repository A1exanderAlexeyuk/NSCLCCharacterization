library(DatabaseConnector)
library(SqlRender)
library(lubridate)


connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "testnode.arachnenetwork.com/synpuf_110k",
                                             user = "ohdsi",
                                             password = 'ohdsi',
                                             port = "5441",
                                             pathToDriver = 'c:/jdbcDrivers')




renderTranslateExecuteSql(connection = conn, 
                        sql = "CREATE TABLE IF NOT EXISTS alex_alexeyuk_results.test_regimens
(
  person_id bigint,
  drug_era_id bigint,
  ingredient character varying(200),
  ingredient_start_date date,
  ingredient_end_date date,
  regimen text,
  regimen_start_date date,
  regimen_end_date date,
  observation_end_date date,
  death_date date
)")
  
  
  
conn <- connect(connectionDetails) 
renderTranslateExecuteSql(connection = conn, sql =   
"INSERT INTO alex_alexeyuk_results.test_regimens(
    person_id, drug_era_id, ingredient, 
    ingredient_start_date, 
    ingredient_end_date, 
    regimen, regimen_start_date, 
    regimen_end_date, observation_end_date, death_date)
  VALUES 
  (3,	17830152,	'drug',	'2000-01-01',	'2000-01-01',	'Regimen2',	'2002-01-01',	    null      ,       null    , '2002-01-10'),
  (4,	17830152,	'drug',	'2000-01-01',	'2000-01-01',	'Regimen3',	'2003-01-01',	'2003-02-01',	   null       ,       null    ),
  (4,	17830152,	'drug',	'2000-01-01',	'2000-01-01',	'Regimen1',	'2003-02-01',	    null      ,	'2003-03-10',	    null      )
  

  
  "
  )
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  