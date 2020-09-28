#cython: language_level=3

#import sys
import pytest
import dbe
import ctypes
import pandas
import pyarrow
import numpy
import shutil
ctypes._dlopen('libDBEngine.so', ctypes.RTLD_GLOBAL)

data_path = "test_dbe_data"
#######################Check init with wrong parameter
def test_failed_init():
    try:
        shutil.rmtree(data_path)
    except FileNotFoundError:
        pass

    with pytest.raises(RuntimeError) as excinfo:
        def f():
            engine = dbe.PyDbEngine(**{"port": 5555, "path": "/"+data_path})
            with pytest.raises(RuntimeError) as excinfo:
                def ff():
                    engine.check_closed()
                f()
            assert "uninitialized" in str(excinfo.value)
        f()
    assert "Permission denied" in str(excinfo.value)

######################Check init with right parameters
def test_success_init():
    global engine
    engine = dbe.PyDbEngine(**{"port": 5555, "path": data_path})
    assert bool(engine.closed) == False

engine = None

######################Check DDL statement
def test_success_DDL():
    engine.executeDDL("create table test (x int not null, w tinyint, y int, z text)")
    assert engine.get_tables() == ['test']

#######################Check creating a duplicate table
def test_failed_DDL():
    with pytest.raises(RuntimeError) as excinfo:
        def f():
            engine.executeDDL("create table test (x int not null, w tinyint, y int, z text)")
        f()
    assert "already exists" in str(excinfo.value)

#######################Check right DML statement
def test_success_DML():
    engine.executeDML("insert into test values(55,5,3,'la-la-la')")
    engine.executeDML("insert into test values(66,6,1, null)")
    engine.executeDML("insert into test values(77,7,null,null)")
    dframe = engine.select_df("select * from test")
    df = pandas.DataFrame({'x': [55,66,77], 'w': [5,6,7], 'y': [3,1,numpy.nan], 'z': ['la-la-la', numpy.nan, numpy.nan]})
    assert not dframe.equals(df)

#######################Check wrong DML statement
def test_failed_DML():
    with pytest.raises(ValueError) as excinfo:
        def f():
            cursor = engine.executeDML("selectTT * from test")
        f()
    assert "Parse failed" in str(excinfo.value)

#######################Check zero division exception
def test_zero_division():
    with pytest.raises(RuntimeError) as excinfo:
        def f():
            cursor = engine.executeDML("SELECT x / (x - x) FROM test")
        f()
    assert "Division by zero" in str(excinfo.value)
