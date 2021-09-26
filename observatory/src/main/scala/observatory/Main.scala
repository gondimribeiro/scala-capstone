package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Extraction._
import observatory.Visualization.{parVisualize, predictTemperature}

import java.time.LocalDate
import scala.collection.parallel.ParSeq

object Main extends App {
  val year = 2021
  val root = "/"
  val stationsPath = s"${root}test_stations.csv"
  val temperaturePath = s"${root}test_temperatures.csv"

  val res = locateTemperatures(year, stationsPath, temperaturePath)
  val temps = locationYearlyAverageRecords(res)
//
//  println(predictTemperature(temps, Location(34, 58)))

//  parVisualize(temps.toSeq.par, Seq((0.1, Color(1, 2, 3))).par)

  val imageArray = Array.fill(4 * 3)(0)
  val locs = Seq((0, 0, 1), (1, 0, 2))

  locs.foreach(t => imageArray(4 * t._1 + t._2) = t._3)

  imageArray.map(print)
//  val image = Image(w=360,h=180,pixels=imageArray)
//  image.output(new java.io.File("target/some-image.png")).
}
