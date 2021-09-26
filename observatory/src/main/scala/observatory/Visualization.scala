package observatory

import com.sksamuel.scrimage.{Image, Pixel}

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

    val (minDistance, tempAtMinDistance) = distances.minBy(_._1)
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
    val (maxTemp, maxColor) = points.maxBy(_._1)

    if (value >= maxTemp) maxColor
    else {
      val (minTemp, minColor) = points.minBy(_._1)

      if (value <= minTemp) minColor
      else {
        val precision = 1e-5

        val (x0, y0) = points
          .filter(_._1 <= value)
          .maxBy(_._1)

        if (math.abs(value - x0) < precision) y0
        else {
          val (x1, y1) = points
            .filter(_._1 >= value)
            .minBy(_._1)

          if (math.abs(value - x1) < precision) y1
          else {
            def interpolate(y0: Int, y1: Int): Int =
              math.round(y0 + ((value - x0) * (y1 - y0)) / (x1 - x0)).toInt

            Color(
              interpolate(y0.red, y1.red),
              interpolate(y0.green, y1.green),
              interpolate(y0.blue, y1.blue)
            )
          }
        }
      }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    //  val parBodies = bodies.par
    //  parBodies.tasksupport = taskSupport
    parVisualize(temperatures.toSeq.par, colors)
  }

  def parVisualize(temperatures: ParSeq[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val lats = (-89 to 90).toSeq.par
    val lons = (-180 to 179).toSeq.par

    val pixels = for {
      lat <- lats
      lon <- lons
    } yield (Location(lat, lon))

    def locationToPixel(location: Location): Pixel = {
      val color = interpolateColor(colors, parPredictTemperature(temperatures, location))
      Pixel(color.red, color.green, color.blue, 255)
    }

    val imageArray = Array.fill(360 * 180)(Pixel(0, 0, 0, 0))
    pixels
      .map(t => ((90 - t.lat).toInt, (t.lon + 180).toInt, locationToPixel(t)))
      .foreach(t => imageArray(360 * t._1 + t._2) = t._3)

    Image(w = 360, h = 180, pixels = imageArray)
  }

  /**
    * @param precision Double precision to comparisons
    * @param loc1      Location 1
    * @param loc2      Location 2
    * @return Distance between locations
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

