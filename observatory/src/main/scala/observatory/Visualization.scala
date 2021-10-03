package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.parallel.ParIterable
import math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {
  val doublePrecision = 1e-6
  val globalP = 2
  val earthRadius = 6.378

  /**
    * @param loc1 Location 1
    * @param loc2 Location 2
    * @return Distance between locations
    */
  def locationsDistance(loc1: Location, loc2: Location): Double = {
    val deltaSigma = {
      // Are equal
      if (abs(loc1.lat - loc2.lat) < doublePrecision &&
        abs(loc1.lon - loc2.lon) < doublePrecision) 0

      // Are antipodes
      else if (abs(loc1.lat + loc2.lat) < doublePrecision &&
        abs(loc1.lon - loc2.lon) < 180 + doublePrecision) Pi

      // Otherwise
      else acos(
        sin(loc1.lat.toRadians) * sin(loc2.lat.toRadians) +
          cos(loc1.lat.toRadians) * cos(loc2.lat.toRadians) *
            cos(loc1.lon.toRadians - loc2.lon.toRadians)
      )
    }

    earthRadius * deltaSigma
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    parPredictTemperature(globalP)(temperatures.par, location: Location)

  def parPredictTemperature(p: Int)(temperatures: ParIterable[(Location, Temperature)], location: Location): Temperature = {
    val distances = temperatures.map {
      case (station, temperature) => (locationsDistance(station, location), temperature)
    }

    val (minDistance, tempAtMinDistance) = distances.minBy(_._1)
    if (minDistance < 1) tempAtMinDistance
    else {
      def inverseDistance(distance: Double): Double =
        1.0 / pow(distance, p)

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
    val equal = points.filter(p => abs(p._1 - value) < doublePrecision)
    if (equal.nonEmpty) equal.head._2
    else {
      val (lower, higher) = points.partition(_._1 < value)
      if (lower.isEmpty) higher.minBy(_._1)._2
      else if (higher.isEmpty) lower.maxBy(_._1)._2
      else {
        val (x0, y0) = lower.maxBy(_._1)
        val (x1, y1) = higher.minBy(_._1)

        def interpolate(y0: Int, y1: Int): Int =
          (y0 + (value - x0) * (y1 - y0) / (x1 - x0)).round.toInt

        Color(
          interpolate(y0.red, y1.red),
          interpolate(y0.green, y1.green),
          interpolate(y0.blue, y1.blue)
        )
      }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image =
      parVisualize(6)(temperatures.par, colors)

  def parVisualize(p: Int)(temperatures: ParIterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    def latAndLonToPixel(lat: Int, lon: Int): Pixel = {
      val color = interpolateColor(colors, parPredictTemperature(p)(temperatures, Location(lat, lon)))
      Pixel(color.red, color.green, color.blue, 255)
    }

    val coordinates = for {
      lat <- 90 to -89 by -1
      lon <- -180 to 179
    } yield (lat, lon)

    val pixels = coordinates.par
      .map { case (lat, lon) => latAndLonToPixel(lat, lon) }
      .toArray

    Image(w = 360, h = 180, pixels = pixels)
  }
}

