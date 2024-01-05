#大数据计算基础大作业

集成aqo（https://github.com/postgrespro/aqo）实现的数据库中间件，可以导出数据元信息（表名、列名、列数、列类型、基数），查询元信息（未优化的逻辑查询计划树及内部谓词）并且将aqo优化后的查询计划提交

##环境搭建
0. 实验环境为Ubuntu 18.04.6 LTS
1. 首先需要根据对应版本的psql下载对应分支的aqo，并按照其中步骤进行安装
2. 安装完成后，在psql中创建用户postgres，并设定密码为123：
`CREATE USER postgres WITH PASSWORD '123';`
3. 使用pgbench生成表，用于测试sql查询优化
`pgbench -p 5432 -h (unix domain socket) -i postgres -s 500`
4. 使用pip3进行依赖库的下载
`pip install sqlalchemy`
`pip install psycopg2`
5. 运行即可