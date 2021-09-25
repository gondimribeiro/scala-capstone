package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Spark.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val temperaturesDF = temperatures.map(t => (t._1.lat, t._1.lon, t._2))
      .toSeq
      .toDF("lat", "lon", "temperature")

    val predictedTemperature = Seq((location.lat, location.lon))
      .toDF("loc_lat", "loc_lon")
      .transform(sparkPredictTemperature(2)(temperaturesDF))
      .select("predicted_temperature")
      .take(1)
      .map(_.getDouble(0))

    predictedTemperature(0)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

  /**
    * @param precision Double precision to comparisons
    * @param df        Spark dataframe with ("lat", "lon", "loc_lat", "loc_lon")
    * @return Spark dataframe with "distance" column indicating the distance between locations
    */
  def sparkWithDistances(precision: Double)(df: DataFrame): DataFrame = {
    val areEqual = abs(col("lat") - col("loc_lat")) < precision &&
      abs(col("lon") - col("loc_lon")) < precision

    val areAntipodes = abs(col("lat") + col("loc_lat")) < precision &&
      abs(col("lon") - col("loc_lon")) < 180 + precision

    val distance = lit(6378) * when(areEqual, 0)
      .when(areAntipodes, scala.math.Pi)
      .otherwise(
        acos(
          sin(radians("lat")) * sin(radians(col("loc_lat"))) +
            cos(radians("lat")) * cos(radians(col("loc_lat"))) *
              cos(radians("lon") - radians(col("loc_lon")))
        )
      )

    df.withColumn("distance", distance)
  }

  /**
    * @param p            Double indicating power of weighted distances
    * @param temperatures Spark dataframe with ("lat", "lon", "temperature")
    * @param locations    Spark dataframe with ("loc_lat", "loc_lon") to predict temperatures on
    * @return Spark dataframe with "predicted_temperature" column
    */
  def sparkPredictTemperature(p: Int)(temperatures: DataFrame)(locations: DataFrame): DataFrame = {
    val predictedTemperature = when(col("min_distance") < 1000.0, col("min_distance_temperature"))
      .otherwise(col("numerator") / col("denominator"))

    temperatures
      .crossJoin(locations)
      .transform(sparkWithDistances(1e-4))
      .withColumn("weight", lit(1.0) / pow(col("distance"), lit(p)))
      .withColumn("numerator", col("weight") * col("temperature"))
      .groupBy("loc_lat", "loc_lon")
      .agg(
        min(col("distance")).as("min_distance"),
        min(expr("struct(distance, temperature).temperature")).as("min_distance_temperature"),
        sum(col("numerator")).as("numerator"),
        sum(col("weight")).as("denominator")
      )
      .withColumn("predicted_temperature", predictedTemperature)
  }

  /**
    * @param points Spark dataframe with ("lat", "lon", "temperature")
    * @param values    Spark dataframe with ("loc_lat", "loc_lon") to predict temperatures on
    * @return Spark dataframe with "predicted_temperature" column
    */
  def sparkInterpolateColors(points: DataFrame)(values: DataFrame): DataFrame = {

    values
      .join(points, values("val_temperature") >= points("temperature") || values("val_temperature") <= points("temperature"))
      .groupBy("val_temperature")
      .agg(
        min(expr("struct(temperature, R).R").as("minR")),
        min(expr("struct(temperature, G).G").as("minG")),
        min(expr("struct(temperature, B).B").as("minB")),
        max(expr("struct(temperature, R).R").as("minR")),
        max(expr("struct(temperature, G).G").as("minG")),
        max(expr("struct(temperature, B).B").as("minB")),
      )
  }
}

