package jobpreparation.scala_workout

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{ col, udf }

object practiceDFOGIT {
  def main(args: Array[String]) {
    {
      val sparkSession = SparkSession.builder.appName("practiceDF").master("local").getOrCreate()
      import sparkSession.sqlContext.implicits._
      val sc = sparkSession.sparkContext
      val sqlctxt = sparkSession.sqlContext
      sc.setLogLevel("ERROR")

      val ReadDF = sqlctxt.read.option("header", "true").option("delimiter", ",").csv("file:///home/hduser/sparkdata/usdata.csv")
      ReadDF.show(10, false)
      //val concvalue = sparkSession.udf.register("concat", (first: String, second: String) => { first + " " + second })
      //val addclm = ReadDF.withColumn("FULL NAME", concvalue($"first_name", $"last_name")).show(100, true)
    }
  }
}