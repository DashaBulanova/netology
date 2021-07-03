from pyspark.sql import SparkSession

spark = (SparkSession
	.builder
	.appName("test cluster mode")
	.config("spark.ui.enabled", "true")
	.config("spark.sql.shuffle.partitions", 6)
	.getOrCreate())
tableA = spark.range(200000000)
tableB = spark.range(100000000)
result = (tableA
	.join(tableB, tableA.id==tableB.id)
	.groupBy()
	.count())

result.collect()

input("Press Enter to continue...")