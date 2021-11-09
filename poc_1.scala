import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}

object poc_1 {
  def main(args: Array[String])
  {
   // System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    val spark = SparkSession
      .builder()
      .appName("Spark POC-1")
      .config("spark.master", "local")
      .getOrCreate()

    var df = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("gs://equifax-poc-3//sparkdemo.csv")
    df.show()
    val colName="Name"
    var df2 = df.withColumn(colName, upper(col(colName)))
    val columns = Seq("Name","Age")
    val df3 = df2.toDF(columns:_*)
    df3.write.option("header",true).csv("gs://equifax-poc-3//output")

  }
}
