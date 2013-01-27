#!/bin/bash

./TestAppendKeyValue.pg
./TestUpdateKeyValue.pg
./TestDeleteKeyValue.pg
./TestSelectKeyValue.pg

./TestAppendJSON.pg
./TestUpdateJSON.pg
./TestDeleteJSON.pg
./TestSelectJSON.pg

./TestConcurrentTransactions.pg
