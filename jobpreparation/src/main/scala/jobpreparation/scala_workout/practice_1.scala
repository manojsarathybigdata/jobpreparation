package jobpreparation.scala_workout

import org.apache.spark.sql.SparkSession

object practice_1 {
  case class Customerdt(name: String, city: String, age: Int)
  def main(args: Array[String]) {
    {
      val sparkSession = SparkSession.builder.appName("practice_1").master("local").getOrCreate()
      import sparkSession.sqlContext.implicits._
      val sc = sparkSession.sparkContext
      val sqlctxt = sparkSession.sqlContext
      sc.setLogLevel("ERROR")
      
      val hadooplines = sc.textFile("hdfs://localhost:54310/user/hduser/empdata.txt")
      val lines = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      val alllines = sc.textFile("file:/home/hduser/sparkdata/*data.txt")
      println("Printinghadooplines");
      hadooplines.foreach { println }
      println("Printinglines");
      lines.foreach { println }
      println("Printingalllines");
      alllines.foreach { println }
      val filterlines = lines.filter { x => x.length().equals(32) }
      val lengths = { lines.map(x => x.split(",")).map(l => l.length) }
      val fmrdd = lines.flatMap(l => l.split(",")).map(x => x.toString.toUpperCase)
      val lengths1 = lines.mapPartitions(x => x.filter(l => l.length > 20))
      val linesFile1 = sc.textFile("file:/home/hduser/sparkdata/empdata.txt")
      val linesFile2 = sc.textFile("file:/home/hduser/sparkdata/empdata1.txt")
      val linesFromBothFiles = linesFile1.union(linesFile2)
      val linesFromCommon = linesFile1.intersection(linesFile2)
      val linesdiffer = linesFile1.subtract(linesFile2)
      val uniqueNumbers = linesFromBothFiles.distinct
      val customers = lines.map(x => x.split(",")).map(y => Customerdt(y(0), y(1), y(2).toInt))
      customers.collect
      val groupbyzip = customers.groupBy { _.city }
      groupbyzip.foreach(println)
      val numbers = sc.parallelize((1 to 100).toList, 15)
      val redpart = numbers.coalesce(2)
      println("partition size is: " + redpart.partitions.size)
      redpart.foreach { println }
      val numbersWithFourPartition = numbers.repartition(6)
      println("Reppartition results:" + numbersWithFourPartition.partitions.size)
      numbersWithFourPartition.foreach { println }
      
      val ReadDF=sqlctxt.read.option("header","true").option("inferschema", "true").option("delimiter",",").csv("file:///home/hduser/sparkdata/usdata.csv")
      
      
    }
  }
}