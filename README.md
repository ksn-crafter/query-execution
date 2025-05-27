# query-execution
Query Execution


In order to run the test case you will have to add the following VM options:
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED

You can also pass these parameters when running test from command line:
ex: mvn test -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"
