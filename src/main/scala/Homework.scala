object Homework extends App {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val path = if (args.length > 0) args(0)
  else "Sample100.csv"

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  val filePrepared = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)

  val dfSorted = filePrepared
    .sort(desc("leave")).show()

  val dfGrouped = filePrepared
    .groupBy("Company Name").max()

  val output = dfGrouped
    .write.csv("newfile.csv")

}
