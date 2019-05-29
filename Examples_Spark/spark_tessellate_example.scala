import org.apache.spark.sql.functions.udf

case class Point(x: Double, y: Double)

class Rectangle(xMin: Double, yMin: Double, xMax: Double, yMax: Double) {
    val bottomLeft = new Point(xMin, yMin)
    val bottomRight = new Point(xMax, yMin)
    val topLeft = new Point(xMin, yMax)
    val topRight = new Point(xMax, yMax)
    
    def intersects(other: Rectangle): Boolean  =
        (this contains other.bottomLeft) || (this contains other.bottomRight) ||
        (this contains other.topLeft) || (this contains other.topRight) ||
        (other contains this.bottomLeft) || (other contains this.bottomRight) ||
        (other contains this.topLeft) || (other contains this.topRight)
    
    def contains(pt: Point) = pt match {case Point(x, y) =>
        xMin <= x && x <= xMax && yMin <= y && y <= yMax
    }
}

case class Cell(cellId: Int, xMin: Double, yMin: Double, xMax: Double, yMax: Double)
    extends Rectangle(xMin, yMin, xMax, yMax)

class Grid(xMinGrid: Double, yMinGrid: Double, xMaxGrid: Double, yMaxGrid: Double, nX: Int, nY: Int) {
    val cells = {
        val stepX = (xMaxGrid - xMinGrid) / nX
        val stepY = (yMaxGrid - yMinGrid) / nY
        for (cellId <- 0 until nX * nY) yield {
            val iX = cellId % nX
            val iY = cellId / nX
            val xMinCell = xMinGrid + iX * stepX
            val yMinCell = yMinGrid + iY * stepY
            println(cellId, xMinCell, yMinCell, xMinCell + stepX, yMinCell + stepY)
            new Cell(cellId, xMinCell, yMinCell, xMinCell + stepX, yMinCell + stepY)
        }
    }
    def getIntersectedCellIds(rect: Rectangle) = cells
        .filter(cell => cell intersects rect)
        .map{case Cell(cellId, _, _, _, _) => cellId}
}

// TODO: передавать grid как параметр
def tessellate(xMinObj: Double, yMinObj: Double, xMaxObj: Double, yMaxObj: Double,
               xMinGrid: Double, yMinGrid: Double, xMaxGrid: Double, yMaxGrid: Double,
               nGridX: Int, nGridY: Int) = {
    // создание сетки
    val grid = new Grid(xMinGrid, yMinGrid, xMaxGrid, yMaxGrid, nGridX, nGridY)
    // объект
    val obj = new Rectangle(xMinObj, yMinObj, xMaxObj, yMaxObj)
    // id ячеек с которыми пересекается объект
    grid.getIntersectedCellIds(obj)
}

def udfTessellate = udf(tessellate _)




//
// пример использования
//
import org.apache.spark.sql.functions.{explode, lit}
val df_vertice_tessellated = sqlContext
    .table("developers.uv_graph_vertice_in_boundaries_selected")
    .repartition(1000, $"vertice_id")
    .withColumn(
        "cell_id", 
        explode(udfTessellate($"lon" - dLon, $"lat" - dLat, $"lon" + dLon, $"lat" + dLat,
                              lit(37.213049), lit(55.482347), lit(37.993007), lit(55.960384), lit(nCells), lit(nCells)))
    )