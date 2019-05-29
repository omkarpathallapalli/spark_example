package net.example.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

package writer {

  trait write {
    def write(writer: Map[String, String], data: Dataset[Row])
  }

  class hivewriter extends write {
    def write(writer: Map[String, String], data: Dataset[Row]) = {
      data.write.mode(SaveMode.Append).insertInto(writer.get("table_name").get)
    }
  }

  class csvwriter extends write {
    def write(writer: Map[String, String], data: Dataset[Row]) = {
      data.write.csv(writer.get("path").get)
    }
  }
}