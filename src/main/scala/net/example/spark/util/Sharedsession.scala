package net.example.spark.util

import scala.util.{ Try, Success, Failure }
import org.apache.spark.sql.SparkSession

trait Sharedsession {
  def cleanly[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
    try {
      Success(doWork(resource))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      try {
        if (resource != null) {
          cleanup(resource)
        }
      } catch {
        case e: Exception => println(e)
      }
    }
  }

}