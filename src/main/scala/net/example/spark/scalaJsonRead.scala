package net.example.spark

import java.io.File

import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object scalaJsonRead extends App {

  var path = "C:/Users/596308/Downloads/Spark-Scala-Maven-Example-master/Spark-Scala-Maven-Example-master/src/test/resources/json/ReadJson.json"

  private[example] sealed case class JsonConf(
    jobname:      String,
    source_read:  String,
    target_write: String,
    reader:       Map[String, String],
    transformer:  Map[String, String],
    writer:       Map[String, String])

  def get(path: String): JsonConf = {

    val mapper = new ObjectMapper
    val stream = Source.fromFile(new File(path))
    val plan = stream.mkString
    println(s"${plan}")
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(plan, classOf[JsonConf])

  }

  val data = get(path)
  val reader_query = s"${data.reader.get("query").getOrElse("No")}"
  val transformer_query = s"${data.transformer.get("query_1").getOrElse("No")}"
  val writer_query = s"${data.writer.get("query").getOrElse("No")}"
}