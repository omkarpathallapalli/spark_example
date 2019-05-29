package net.example.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

package reader {

  trait read {
    def read(reader: Map[String, String]): Dataset[Row]
  }

  class hivereader(session: SparkSession) extends read {
    def read(reader: Map[String, String]): Dataset[Row] = {
      session.sql(reader.get("query").get)
    }
  }
  
  class jsonreader(session: SparkSession) extends read {
    def read(reader: Map[String, String]): Dataset[Row] = {
      session.read.json(reader.get("path").get)
    }
  }

}