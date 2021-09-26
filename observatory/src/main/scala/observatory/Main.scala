package observatory

import com.sksamuel.scrimage.writer
import observatory.Extraction._
import observatory.Visualization._


object Main extends App {

  def printToc(t: Long): Unit = {
    val duration: Double = (System.nanoTime - t) / 1e9d
    val minutes: Int = (duration / 60).toInt
    val seconds: Int = math.round(duration % 60).toInt
    println(s"\telapsed time: ${minutes}min ${seconds}sec")
  }

  val year = 1975
  val root = "/"
  val stationsPath = s"${root}stations.csv"
  val temperaturePath = s"${root}${year}.csv"
  val temperatureColorsPath = s"${root}temperature_colors.csv"

  println("Locating temperatures...")
  var tic = System.nanoTime
  val locations = parLocateTemperatures(year, stationsPath, temperaturePath)
  printToc(tic)

  println("Computing averages...")
  tic = System.nanoTime
  val temperatures = parLocationYearlyAverageRecords(locations)
  printToc(tic)

  println("Computing colors...")
  tic = System.nanoTime
  val colors = readResource(temperatureColorsPath)
    .map(TemperatureColors)
    .map(c => (c.temperature, Color(c.red, c.green, c.blue)))

  val image = parVisualize(temperatures, colors)
  image.output(new java.io.File(s"target/image-$year.png"))
  printToc(tic)
}
