Please note, that this is my first project uploaded and managed via GitHub. 
If you have any suggestions or improvements, feel free to contact me at:

    riemersebastian@hotmail.com

This project aims at benchmarking different nosql/sql databases using a single java api.

Currently, three databases are supported: 
- redis
- mongoDB
- mysql

I am currently working on the following test cases:
- insert/update/delete/select simple key/value pairs
- insert/update/delete/select small XML documents
- insert/update/delete/select big XML documents
- concurrent insert/updates/deletes/selects

The output will be datafiles containing load/runtime information which will be plottable using gnuplot.
