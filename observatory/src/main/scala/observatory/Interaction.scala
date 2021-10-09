package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{interpolateColor, predictTemperature}

import math._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {
  val zoomLevel: Int = 8
  val tileSide: Int = 1 << zoomLevel
  val alpha: Int = 100
  val globalP: Int = 6

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val n = (1 << tile.zoom).toDouble
    val lon = 360.0 * tile.x / n - 180.0
    val lat = atan(sinh(Pi - 2 * Pi * tile.y / n)).toDegrees

    Location(lat = lat, lon = lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    System.gc()
    val result = parTile(temperatures, colors, tile)
    System.gc()
    result
  }

  def parTile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val zoomedX = tileSide * tile.x
    val zoomedY = tileSide * tile.y
    val zoom = tile.zoom + zoomLevel

    def tileLocationToPixel(x: Int, y: Int): Pixel = {
      val location = tileLocation(Tile(x = zoomedX + x, y = zoomedY + y, zoom = zoom))
      val color = interpolateColor(colors, predictTemperature(temperatures, location))
      Pixel(color.red, color.green, color.blue, alpha)
    }

    val coordinates = for {
      y <- 0 until tileSide
      x <- 0 until tileSide
    } yield (x, y)

    val pixels = coordinates.par
      .map { case (x, y) => tileLocationToPixel(x, y) }
      .toArray

    Image(w = tileSide, h = tileSide, pixels = pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Year, Data)],
                           generateImage: (Year, Tile, Data) => Unit
                         ): Unit = {
    System.gc()
    generateTiles(0 to 3)(yearlyData, generateImage)
    System.gc()
  }

  def generateTiles[Data](zoomLevels: Iterable[Int])(
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    zoomLevels.foreach { zoom =>
      val mapSide = 1 << zoom
      for {
        x <- 0 until mapSide
        y <- 0 until mapSide
        (year, data) <- yearlyData
      } yield generateImage(year, Tile(x = x, y = y, zoom = zoom), data)
    }
  }
}
