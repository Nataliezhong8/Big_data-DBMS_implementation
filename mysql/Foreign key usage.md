## query plan
In mysql, if you want to check the query plan:  
- if your version is higher than 8.0.20, you can use
  ```
  explain analyze [query]
  ```
  You can check usage here: https://dev.mysql.com/doc/refman/8.0/en/explain.html#explain-analyze
- if your version is lower than 8.0.20 and higher than 5.0.37, you can use "show profiles", steps:
   ```
   select version(); --check your sql version
   show variable like "%pro%"; --check if your profile is on (default setting is off, you need to turn it on every time you want to use)
   set profiling = 1; -- turn on profile
   
   -- you can run your query, e.g
   select * from table A;
   show profiles; -- you can check all execution time for queries and their ids
   show profile for query [id]; -- you can check the detailed execution time for a specific query id
   ```
