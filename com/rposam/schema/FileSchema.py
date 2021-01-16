from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, DateType


class FileSchema:
    def randomuserapiSchema():
        schema = StructType([
            StructField("results", ArrayType(
                StructType([
                    StructField("user", StructType([
                        StructField("gender", StringType()),
                        StructField("name", StructType([
                            StructField("title", StringType()),
                            StructField("first", StringType()),
                            StructField("last", StringType())
                        ])),
                        StructField("location", StructType([
                            StructField("street", StringType()),
                            StructField("city", StringType()),
                            StructField("state", StringType()),
                            StructField("zip", StringType())
                        ])),
                        StructField("email", StringType()),
                        StructField("username", StringType()),
                        StructField("password", StringType()),
                        StructField("salt", StringType()),
                        StructField("md5", StringType()),
                        StructField("sha1", StringType()),
                        StructField("sha256", StringType()),
                        StructField("registered", StringType()),
                        StructField("dob", StringType()),
                        StructField("phone", StringType()),
                        StructField("cell", StringType()),
                        StructField("HETU", StringType()),
                        StructField("picture", StructType([
                            StructField("large", StringType()),
                            StructField("medium", StringType()),
                            StructField("thumbnail", StringType())
                        ]))
                    ]))
                ])
            )),
            StructField("nationality", StringType()),
            StructField("seed", StringType()),
            StructField("version", StringType())
        ])
        return schema

    def _20000recordsSchema():
        schema = StructType([StructField("ID", IntegerType(), False),
                             StructField("Name", StringType(), False),
                             StructField("Email", StringType(), True),
                             StructField("Age", IntegerType(), True),
                             StructField("Gender", StringType(), False),
                             StructField("Salary", DoubleType(), True)])
        return schema

    def empSchema():
        schema = StructType([
            StructField("empno", IntegerType()),
            StructField("ename", StringType()),
            StructField("job", StringType()),
            StructField("mgr", IntegerType()),
            StructField("hiredate", DateType()),
            StructField("sal", DoubleType()),
            StructField("comm", IntegerType()),
            StructField("deptno", IntegerType())
        ])
        return schema
