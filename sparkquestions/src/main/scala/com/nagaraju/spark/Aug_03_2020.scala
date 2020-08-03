package com.nagaraju.spark

import org.apache.spark.sql.SparkSession

object Aug_03_2020 {
  case class Employee(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    // Creating spark Sesstion
    val spark = SparkSession.builder().appName("Aug_03_2020").master("local").getOrCreate()
    //Testing Problem
    import spark.implicits._
   // Creating RDD,we have kept the file schema here because if we run the application in spark submit then it will contact thee HDFC default.
    //So, here we are telling spark driver to get the file from local.
    val employeeRDD = spark.sparkContext.textFile("file:///home/nagaraju/Technology/Repository/nagarajugajula/stackoverflowquestions/sparkquestions/data/employee.txt")
    // Creating header
    val header = employeeRDD.first()
    // Creating Employee DataFrame
    val employeeDF = employeeRDD.filter(line => line!=header).map(_.split(",")).map(attributes => Employee(attributes(0), attributes(1).trim.toInt)).toDF()
    // Creating Temperary View
    employeeDF.createOrReplaceTempView("employee")
   // Using Spark SQL and getting data.
    var youngstersDF = spark.sql("SELECT name,age FROM employee WHERE age BETWEEN 18 AND 30")
    youngstersDF.map(youngster => "Name: " + youngster(0)).show()
  }
}
