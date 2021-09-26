package observatory

import java.time.LocalDate
import scala.io.Source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

import scala.collection.parallel.ParSeq


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
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] =
    locateTemperaturesWithSpark(year, stationsFile, temperaturesFile)

  def locateTemperaturesWithSpark(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Setup Spark
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Observatory")
        .master("local")
        .getOrCreate()
    import spark.implicits._

    spark.conf.set("spark.executor.instances", 8)
    spark.conf.set("spark.executor.cores", 8)

    // Read stations file
    val stationsSchema = new StructType()
      .add("stn_id", LongType, nullable = true)
      .add("wban_id", LongType, nullable = true)
      .add("lat", DoubleType, nullable = true)
      .add("lon", DoubleType, nullable = true)

    val stationsDF = spark
      .read
      .schema(stationsSchema)
      .csv(readResource(stationsFile).toDS)
      .filter(col("lat").isNotNull || col("lon").isNotNull)

    // Read temperatures file
    val temperatureSchema = new StructType()
      .add("stn_id", LongType, nullable = true)
      .add("wban_id", LongType, nullable = true)
      .add("month", StringType, nullable = true)
      .add("day", StringType, nullable = true)
      .add("temperature", DoubleType, nullable = true)

    val temperatureDF = spark
      .read
      .schema(temperatureSchema)
      .csv(readResource(temperaturesFile).toDS)

    // Join dataframes and collect output
    val locatedTemperatures = temperatureDF
      .join(
        stationsDF,
        temperatureDF("stn_id") <=> stationsDF("stn_id") && temperatureDF("wban_id") <=> stationsDF("wban_id"),
        "inner"
      )
      .withColumn("date", to_date(concat_ws("-", lit(year), col("month"), col("day"))))
      .select("date", "lat", "lon", "temperature")
      .withColumn("temperature", (col("temperature") - 32) / 1.80)
      .collect()
      .map(
        row => (
          row.getDate(0).toLocalDate,
          Location(lat = row.getDouble(1), lon = row.getDouble(2)),
          row.getDouble(3)
        )
      )

    spark.close()
    locatedTemperatures
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] =
    parLocationYearlyAverageRecords(records.toArray.par).seq

  def parLocationYearlyAverageRecords(records: ParSeq[(LocalDate, Location, Temperature)]): ParSeq[(Location, Temperature)] = {
    def computeTemperateAverages(temperatures: ParSeq[(LocalDate, Location, Temperature)]): Temperature = {
      val reduced = temperatures
        .map(t => (t._3, 1))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

      reduced._1 / reduced._2
    }

    records
      .groupBy(t => t._2)
      .map(t => (t._1, computeTemperateAverages(t._2)))
      .toSeq
  }

  /**
    * @param resource Resource to read
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def readResource(resource: String): Seq[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    Source.fromInputStream(fileStream).getLines().filter(_ != "").toList
  }
}
