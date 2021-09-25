package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Spark.spark

import scala.collection.parallel.ParSeq

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    parPredictTemperature(temperatures.toSeq.par, location: Location)

  def parPredictTemperature(temperatures: ParSeq[(Location, Temperature)], location: Location): Temperature = {
    val distances = temperatures
      .map(t => (locationsDistance(1e-5)(t._1, location), t._2))

    val (minDistance, tempAtMinDistance) = distances.minBy(_._2)
    if (minDistance < 1000) tempAtMinDistance
    else {
      def inverseDistance(distance: Double): Double =
        1.0 / math.pow(distance, 2)

      val factors = distances
        .map(t => (inverseDistance(t._1), inverseDistance(t._1) * t._2))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

      factors._2 / factors._1
    }
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
  def locationsDistance(precision: Double)(loc1: Location, loc2: Location): Double = {
    val areEqual = math.abs(loc1.lat - loc2.lat) < precision &&
      math.abs(loc1.lon - loc2.lon) < precision

    val areAntipodes = math.abs(loc1.lat + loc2.lat) < precision &&
      math.abs(loc1.lon - loc2.lon) < 180 + precision

    val deltaSigma = {
      if (areEqual) 0
      else if (areAntipodes) math.Pi
      else math.acos(
        math.sin(loc1.lat.toRadians) * math.sin(loc2.lat.toRadians) +
          math.cos(loc1.lat.toRadians) * math.cos(loc2.lat.toRadians) *
            math.cos(loc1.lon.toRadians - loc2.lon.toRadians)
      )
    }

    6378 * deltaSigma
  }
}

