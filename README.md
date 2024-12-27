# PaquetBenchmarkGenerator
This project aims to generate Parquet data files for TPC-H and ClickBench datasets using parquet-java.
The ParquetWriter in pyarrow.parquet depends on pyarrow-cpp, which is outdated and does not support specifying rowGroupSize.

This project is part of PixelsDB.

# Type Conversion
Parquet Logical Types are used to construct the message schema.

# Multi-threaded File Reading
To enable multi-threaded file reading, files need to be split into smaller chunks.
The Linux split command is used for this purpose:

```bash
Copy code
# Test example
split -l 30 test1.csv ./test1/test1-csv-
split -l 40 test2.csv ./test2/test2-csv-

# ClickBench
split -l 156250 hits.tsv ./hits/hits-tsv-

# TPC-H
split -l 319150 customer.tbl ./customer/customer-tbl-
split -l 600040 lineitem.tbl ./lineitem/lineitem-tbl-
split -l 100 nation.tbl ./nation/nation-tbl-
split -l 638300 orders.tbl ./orders/orders-tbl-
split -l 769240 part.tbl ./part/part-tbl-
split -l 360370 partsupp.tbl ./partsupp/partsupp-tbl-
split -l 10 region.tbl ./region/region-tbl-
split -l 333340 supplier.tbl ./supplier/supplier-tbl-
```
# References
- https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
- https://cloud.tencent.com/developer/article/2439115
- https://trino.io/docs/current/language/types.html
- https://javadoc.io/static/org.apache.parquet/parquet-hadoop/1.11.0/org/apache/parquet/hadoop/ParquetWriter.Builder.html
