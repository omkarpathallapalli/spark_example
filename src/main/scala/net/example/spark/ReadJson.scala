package net.example.spark

import org.apache.spark.sql.functions
import net.example.spark.util.Sharedsession
import org.apache.spark.sql.SparkSession

object ReadJson extends SparkEnv {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val employee = spark.read.json("C:\\Users\\596308\\Desktop\\git\\spark_example\\src\\test\\resources\\data\\employees.json")
    val title = spark.read.json("C:\\Users\\596308\\Desktop\\git\\spark_example\\src\\test\\resources\\data\\titles.json")
    val df = employee.join(title, Seq("emp_no"), "inner")
    df.persist().createOrReplaceTempView("df")
    df.show()
  }
}