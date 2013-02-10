This project aims at benchmarking different nosql/sql databases using a single java api.

Currently, four databases are supported: 
- redis
- mongoDB
- mysql
- Cassandra

A couple of test cases have been implemented so far:
- insert/update/delete/select simple key/value pairs
- insert/update/delete/select small XML documents
- concurrent transactions (combination of insert/select/update/delete)

In the folder /out you can find the output of each test case, which is a datafile containing load/runtime information.

In the folder /scripts you can find some gnuplot scripts to generate benchmark diagrams out of the data-files.

In the folder /plots you can find the generated .PNG benchmarks for each test case.


Before executing a benchmark, have a look in NosqlBenchmark.java

Most important, edit the two arrays loadSize and runTime to fit the actual test size.

To execute a benchmark, make sure the database you wish to test is running, and start the project with some command line parameters:

e.g. __NosqlBenchmark__ TestConcurrentTransactions MongoDB

If you have any questions, suggestions or improvements, feel free to contact me at:

    riemersebastian@hotmail.com

