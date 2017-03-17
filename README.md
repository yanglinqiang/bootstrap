# bootstrap

```
CREATE KEYSPACE examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}  AND durable_writes = true;
USE examples ; 
CREATE TABLE data_log(seeds text PRIMARY KEY,table_name text,max_num bigint,create_time bigint);
CREATE TABLE data1(uuid text PRIMARY KEY,col_time bigint);
CREATE TABLE data2(uuid blob PRIMARY KEY,col_time blob);
```


### 遇到的坑
    
    cassandra-driver-core的包，要用服务器上的包，在网上下的包不行，qps达不到要求。