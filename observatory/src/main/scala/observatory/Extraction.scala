package observatory

import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.LocalDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import observatory.Spark.spark
import org.apache.log4j.{Level, Logger}
import spark.implicits._

import java.sql.Date
import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    sparkExtractData(year, stationsFile, temperaturesFile)
      .collect()
      .map(
        row => (
          row.getDate(0).toLocalDate,
          Location(lat=row.getDouble(1), lon=row.getDouble(2)),
          row.getDouble(3)
        )
      )
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val recordsDF = records.map(t => (Date.valueOf(t._1), t._2.lat, t._2.lon, t._3))
      .toSeq
      .toDF("date", "lat", "lon", "temperature")

    sparkAverageRecords(recordsDF)
      .collect()
      .map(
        row => (
          Location(lat=row.getDouble(0), lon=row.getDouble(1)),
          row.getDouble(2)
        )
      )

  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A Spark Dataframe with columns (date, lat, lon, temperature)
    */
  def sparkExtractData(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val stationsSchema = new StructType()
      .add("stn_id", LongType, nullable = true)
      .add("wban_id", LongType, nullable = true)
      .add("lat", DoubleType, nullable = true)
      .add("lon", DoubleType, nullable = true)

    val temperatureSchema = new StructType()
      .add("stn_id", LongType, nullable = true)
      .add("wban_id", LongType, nullable = true)
      .add("month", StringType, nullable = true)
      .add("day", StringType, nullable = true)
      .add("temperature", DoubleType, nullable = true)

    val stationsDF = spark
      .read
      .schema(stationsSchema)
      .csv(getRddFromResource(stationsFile))
      .filter(col("lat").isNotNull || col("lon").isNotNull)

    val temperatureDF = spark
      .read
      .schema(temperatureSchema)
      .csv(getRddFromResource(temperaturesFile))

    temperatureDF
      .join(
        stationsDF,
        temperatureDF("stn_id") <=> stationsDF("stn_id") && temperatureDF("wban_id") <=> stationsDF("wban_id"),
        "inner"
      )
      .withColumn("date", to_date(concat_ws("-", lit(year), col("month"), col("day"))))
      .select("date", "lat", "lon", "temperature")
      .withColumn("temperature", (col("temperature") - 32) / 1.80)
  }

  /**
    * @param df A Spark Dataframe with columns (date, lat, lon, temperature)
    * @return A Spark Dataframe with columns (lat, lon, avg_temperature)
    */
  def sparkAverageRecords(df: DataFrame): DataFrame = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    df
      .groupBy("lat", "lon")
      .agg(avg(col("temperature").as("avg_temperature")))
  }

  /**
    * @param resource A string with a CSV resource path
    * @return A Spark Dataset[String] containing the CSV rows
    */
  def getRddFromResource(resource: String): Dataset[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    spark.sparkContext.makeRDD(Source.fromInputStream(fileStream).getLines().toList).toDS
  }

}
