# ParquetWriter
本项目目标是利用[parquet-java](https://github.com/apache/parquet-java)
生成tpch和clickbench的parquet的数据文件(prarrow.parquet的praquetWriter依赖于prarrow-cpp,版本比较老且无法指定rowGroupSize)

本项目附属于[PixelsDB](https://github.com/pixelsdb)

## 类型转换
采取parquet的Logical Type构造message 

## 多线程读取
为了使用多线程读取文件
需要对文件进行切分
使用linux的 spilt命令
```bash
# test 
split -l 30 test1.csv ./test1/test1-csv-
split -l 40 test2.csv ./test2/test2-csv-

#clickbench 
split -l 156250 hits.tsv ./hits/hits-tsv-

# tpch
split -l 319150 customer.tbl ./customer/customer-tbl-
split -l 600040 lineitem.tbl ./lineitem/lineitem-tbl-
split -l 100 nation.tbl ./nation/nation-tbl-
split -l 638300 orders.tbl ./orders/orders-tbl-
split -l 769240 part.tbl ./part/part-tbl-
split -l 360370 partsupp.tbl ./partsupp/partsupp-tbl-
split -l 10 region.tbl ./region/region-tbl-
split -l 333340 supplier.tbl ./supplier/supplier-tbl-
```

## 参考
- https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
- https://cloud.tencent.com/developer/article/2439115
- https://trino.io/docs/current/language/types.html