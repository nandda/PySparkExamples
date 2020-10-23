from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[2]").appName("JoinsType").getOrCreate()

emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
       (2, "Rose", 1, "2010", "20", "M", 4000),
       (3, "Williams", 1, "2010", "10", "M", 1000),
       (4, "Jones", 2, "2005", "10", "F", 2000),
       (5, "Brown", 2, "2010", "40", "", -1),
       (6, "Brown", 2, "2010", "50", "", -1)
       ]

empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
              "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)
# empDF.show()

dept = [("Finance", 10),
        ("Marketing", 20),
        ("Sales", 30),
        ("IT", 40)
        ]

deptColumns = ["Dept", "DeptId"]

depDF = spark.createDataFrame(data=dept, schema=deptColumns)
# depDF.show()

# Inner Join
# empDF.join(depDF, empDF.emp_dept_id == depDF.DeptId, "Inner").show()

# Full,Full outer,outer

# empDF.join(depDF,empDF.emp_dept_id == depDF.DeptId,"Full").show()

# Left,Left outer

# empDF.join(depDF,empDF.emp_dept_id == depDF.DeptId,"Left").show()

# Right

# empDF.join(depDF,empDF.emp_dept_id == depDF.DeptId,"Right").show()

# Left Semi

# empDF.join(depDF,empDF.emp_dept_id == depDF.DeptId,"leftsemi").show()

# Left Anti

# empDF.join(depDF,empDF.emp_dept_id == depDF.DeptId,"leftanti").show()

# Self Join

empDF.alias("emp1").join(empDF.alias("emp2"),
                         col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
    .select(col("emp1.emp_id"), col("emp1.name"), col("emp2.emp_id").alias("superior_id"),
            col("emp2.name").alias("superior_emp_name")).show()
