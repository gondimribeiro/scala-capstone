package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{interpolateColor, predictTemperature}
import observatory.Interaction.tileLocation

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {
  val zoomLevel: Int = 8
  val tileSide: Int = 1 << zoomLevel
  val alpha: Int = 100

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00   Top-left value
    * @param d01   Bottom-left value
    * @param d10   Top-right value
    * @param d11   Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(point: CellPoint, d00: Temperature, d01: Temperature,
                            d10: Temperature, d11: Temperature): Temperature =
    d00 + (d10 - d00) * point.x + (d01 - d00) * point.y + (d11 - d10 - d01 + d00) * point.x * point.y

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param tile   Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(grid: GridLocation => Temperature, colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    System.gc()
    val result = visualizeGridNoGC(grid, colors, tile)
    System.gc()
    result
  }

  def visualizeGridNoGC(grid: GridLocation => Temperature, colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val zoomedX = tileSide * tile.x
    val zoomedY = tileSide * tile.y
    val zoom = tile.zoom + zoomLevel

    def tileLocationToPixel(x: Int, y: Int): Pixel = {
      val location = tileLocation(Tile(x = zoomedX + x, y = zoomedY + y, zoom = zoom))

      val (originLat, originLon) = (location.lat.ceil.toInt, location.lon.floor.toInt)

      val point = CellPoint(location.lon - originLon, originLat - location.lat)
      val d00 = grid(GridLocation(originLat, originLon))
      val d01 = grid(GridLocation(originLat - 1, originLon))
      val d10 = grid(GridLocation(originLat, originLon + 1))
      val d11 = grid(GridLocation(originLat - 1, originLon + 1))
      val color = interpolateColor(colors, bilinearInterpolation(point, d00, d01, d10, d11))
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

}
