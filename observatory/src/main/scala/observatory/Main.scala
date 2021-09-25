package observatory

import observatory.Extraction._
import observatory.Visualization.predictTemperature

import java.time.LocalDate
import scala.collection.parallel.ParSeq

object Main extends App {
  val year = 2021
  val root = "/"
  val stationsPath = s"${root}test_stations.csv"
  val temperaturePath = s"${root}test_temperatures.csv"

  val res = locateTemperatures(year, stationsPath, temperaturePath)
  val temps = locationYearlyAverageRecords(res)

  println(predictTemperature(temps, Location(34, 58)))
}
