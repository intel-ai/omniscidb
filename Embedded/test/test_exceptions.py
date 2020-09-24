#cython: language_level=3

#import sys
import dbe
import ctypes
import pandas
import pyarrow
import numpy
ctypes._dlopen('libDBEngine.so', ctypes.RTLD_GLOBAL)


print("\n*****************Check init with wrong parameter...")
try:
    obj = dbe.PyDbEngine(**{"port": 5555, "path": "/data_test", "arg1": 5})
    obj.executeDDL("create table test (x int not null, w tinyint, y int, z text)")
    tbl = obj.get_tables()
except RuntimeError as e:
    print("Runtime error: {0}".format(e))

print("\n*****************Check init with right parameters...")
try:
    obj = dbe.PyDbEngine(**{"port": 5555, "path": "data_test"})
    print("DBE initialized")
    print("\n*****************Check first create table statement")
    obj.executeDDL("create table test (x int not null, w tinyint, y int, z text)")
    print("Table created")
    print(obj.get_tables())
    print(obj.get_table_details("test"))
    print("\n*****************Check creating a duplicate table")
    obj.executeDDL("create table test (x int not null, w tinyint, y int, z text)")
    print("Table created")
    print(obj.get_tables())
except RuntimeError as e:
    print("Error: {0}".format(e))


print("\n*****************Check right DML statement")
try:
    obj.executeDML("insert into test values(55,5,3,'la-la-la')")
    obj.executeDML("insert into test values(66,6,1, null)")
    obj.executeDML("insert into test values(77,7,null,null)")
    dframe = obj.select_df("select * from test")
    print(dframe)
except ValueError as e:
    print("Runtime error: {0}".format(e))


print("\n*****************Check wrong DML statement")
try:
    df = obj.select_df("selectTT * from test")
    print(df)
except ValueError as e:
    print("Runtime error: {0}".format(e))

print("\n*****************Check zero division exception")
try:
    cursor = obj.executeDML("SELECT x / (x - x) FROM test")
    print(cursor)
except RuntimeError as e:
    print("Runtime error: {0}".format(e))

print('\n*****************END')