import os
import random


def create_files_based_on_schema(source_path, schema):
    """
    根据给定的模式（schema）在指定源路径下创建目录和文件，并写入示例数据。

    :param source_path: 源路径，即基准的父目录路径
    :param schema: 包含表名和对应字段类型的模式字典，例如 {"test1": ["int32", "int32", "float"],...}
    """
    for table_name, field_types in schema.items():
        # 创建表名对应的目录
        table_dir = os.path.join(source_path, table_name)
        os.makedirs(table_dir, exist_ok=True)

        # 构建文件路径（这里以CSV文件为例，文件名假设为 data.csv，你可按需调整）

        file_path = os.path.join(table_dir, table_name+".csv")

        # 打开文件并写入100行数据
        with open(file_path, 'w') as f:
            for _ in range(100):
                row_data = []
                for field_type in field_types:
                    if field_type == "int32":
                        row_data.append(str(random.randint(-1000, 1000)))
                    elif field_type == "int64":
                        row_data.append(str(random.randint(-1000000, 1000000)))
                    elif field_type == "float":
                        row_data.append(str(random.uniform(-100.0, 100.0)))
                    elif field_type == "boolean":
                        row_data.append(str(random.choice([True, False])))
                    elif field_type == "double":
                        row_data.append(str(random.uniform(-1000.0, 1000.0)))
                f.write(','.join(row_data) + '\n')


if __name__ == "__main__":
    source_path = "/home/whz/ParquetWriter/input"
    schema = {
        "test1": ["int32", "int32", "float"],
        "test2": ["boolean", "int32", "int64", "double"]
    }
    create_files_based_on_schema(source_path, schema)