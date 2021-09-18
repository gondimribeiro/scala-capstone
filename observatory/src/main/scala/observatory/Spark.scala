package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark {
  // Setup Spark
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .master("local")
      .getOrCreate()

  spark.conf.set("spark.executor.instances", 4)
  spark.conf.set("spark.executor.cores", 4)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
}
