----------------------------------------------------------
--  title	java-nosql-benchmark 
--  author	Sebastian Riemer, TU Wien, Austria
--  date	12.12.2012
----------------------------------------------------------

Under construction!

Please note, that this is my first project uploaded and managed via GitHub. 
If you have any suggestions or improvements, feel free to contact me at:

    riemersebastian@hotmail.com

This project aims at benchmarking different nosql/sql databases using a single java api.

Currently, three databases are supported: 
- redis
- mongoDB
- mysql

I am currently working on the following test cases:
- insert simple key/value pairs
- insert small XML documents
- insert big XML documents

The output will be datafiles containing load/runtime information which will be plottable using gnuplot.
