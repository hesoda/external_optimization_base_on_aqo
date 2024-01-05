from sqlalchemy import create_engine, MetaData, Table, inspect, text
from sqlalchemy.orm import sessionmaker
import re
host, database, user, password, port = 'localhost', 'postgres', 'postgres', '123', '5432'   # password -2

engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (user, password, host, port, database))
connect = engine.connect()


def getMetadata(schema='public'):
    # 获取元数据
    insp = inspect(engine)
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=schema)    # 此处可以设置函数，令模式名称需要输入
    return insp, metadata

# print(metadata.tables)
session = sessionmaker(bind=engine)()
# 获取表名
# for table in insp.get_table_names(schema='test'):
#     countColumn = 0
#     print(table)
#     for column in insp.get_columns(table, schema='test'):
#         print(column)
#         countColumn += 1
#     print('table:'+table+"'s column:"+str(countColumn))
#     table_dict = {i.name: i for i in metadata.tables.values()}
#     print('元组数量：'+str(len(session.query(table_dict[table]).all())))

# 查询计划树
# table_name = 'test.tes'
# query_plan = connect.exec_driver_sql(f"EXPLAIN (FORMAT JSON) SELECT * FROM {table_name}")  # 采用函数传参输入sql语句,sql语句可作为输入，将其封装入函数中
# plan_tree = query_plan.scalar()  # 查询元信息在plan_tree中
#
# print("Query Plan Tree:")
# print(plan_tree)

patten = r"Node Type"
s = 'select * from pgbench_accounts as a, pgbench_branches as b, pgbench_tellers as t where a.bid = b.bid and b.bid = t.bid and a.aid < t.tid and t.tid in (1, 2, 3, 4, 5, 6) and a.bid = 1 and b.bid = 1;'


def getPrimaryTree(sql=s):
    connect.exec_driver_sql("SET enable_seqscan = off;")
    connect.exec_driver_sql("SET enable_indexscan = off;")
    connect.exec_driver_sql("SET enable_bitmapscan = off;")
    connect.exec_driver_sql("SET geqo=false;")
    query_plan = connect.exec_driver_sql("EXPLAIN (FORMAT JSON)" + sql + ';')
    plan_tree = query_plan.scalar()
    print("PRIMARY QUERY PLAN TREE:")
    s = str(plan_tree)
    print(s)
    pStr = str(plan_tree)
    print(re.findall(r"'Node Type': '(.*?)'", pStr, re.DOTALL))


def getTree(sql=s):
    connect.exec_driver_sql("SET enable_seqscan = true;")
    connect.exec_driver_sql("SET enable_indexscan = true;")
    connect.exec_driver_sql("SET enable_bitmapscan = true;")
    connect.exec_driver_sql("set geqo=true;")
    query_plan = connect.exec_driver_sql("EXPLAIN (FORMAT JSON)" + sql + ';')
    plan_tree = query_plan.scalar()
    print("QUERY PLAN TREE:")
    print(plan_tree)
    pStr = str(plan_tree)
    print(re.findall(r"'Node Type': '(.*?)'", pStr, re.DOTALL))


def commit(sql=s):
    connect.exec_driver_sql("SET aqo.mode = 'learn';")
    connect.exec_driver_sql("EXPLAIN analyze " + sql + ';')
    for i in range(0, 10):
        connect.exec_driver_sql("EXPLAIN analyze " + sql + ';')
        connect.exec_driver_sql("RESET aqo.mode;")
    print(session.execute(text(sql)).fetchall())


def getTable(schema='public'):
    insp, metadata = getMetadata(schema)
    for table in insp.get_table_names(schema=schema):
        countColumn = 0
        print(table)
        for column in insp.get_columns(table, schema=schema):
            print(column)
            countColumn += 1
        print('table:' + table + "'s column:" + str(countColumn))
        table_dict = {i.name: i for i in metadata.tables.values()}
        print('元组数量：' + str(len(session.query(table_dict[table]).all())))


getTable()
getPrimaryTree()
getTree()
commit()
