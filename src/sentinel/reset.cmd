copy /v/y orig\redis-6380\* redis-6380
copy /v/y orig\redis-6381\* redis-6381
copy /v/y orig\redis-6382\* redis-6382

del /F /Q redis-6380\dump.rdb
del /F /Q redis-6381\dump.rdb
del /F /Q redis-6382\dump.rdb
