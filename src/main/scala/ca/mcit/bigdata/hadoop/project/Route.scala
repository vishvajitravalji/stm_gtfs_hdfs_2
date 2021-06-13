package ca.mcit.bigdata.hadoop.project

case class Route(
                  routeId: Int,
                  routeLongName: String,
                  routeColor: String
                )
object Route{
  def toCsv(route: Route): String = {
    route.routeId + "," +
      route.routeLongName + "," +
      route.routeColor
  }
}