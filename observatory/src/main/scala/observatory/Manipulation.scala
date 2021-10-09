package observatory

import observatory.Visualization.predictTemperature

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {
  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    System.gc()
    makeGridNoGC(temperatures)
  }

  def makeGridNoGC(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val locationTemperature: mutable.Map[GridLocation, Temperature] = new TrieMap[GridLocation, Temperature]()

    location: GridLocation =>
      locationTemperature.get(location) match {
        case Some(temperature) => temperature
        case None =>
          val temperature = predictTemperature(temperatures, Location(location.lat, location.lon))
          locationTemperature.put(location, temperature)
          temperature
      }
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    System.gc()
    averageNoGC(temperaturess)
  }

  def averageNoGC(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val numberYears: Int = temperaturess.size
    val locationTemperature: mutable.Map[GridLocation, Temperature] = new TrieMap[GridLocation, Temperature]()

    location: GridLocation =>
      locationTemperature.get(location) match {
        case Some(temperature) => temperature
        case None =>
          val temperature = temperaturess
            .map(predictTemperature(_, Location(location.lat, location.lon)))
            .sum / numberYears

          locationTemperature.put(location, temperature)
          temperature
      }
  }


  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    System.gc()
    deviationNoGC(temperatures, normals)
  }

  def deviationNoGC(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    location: GridLocation => makeGridNoGC(temperatures)(location) - normals(location)
  }

}

