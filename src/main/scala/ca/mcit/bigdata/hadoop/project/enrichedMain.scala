package ca.mcit.bigdata.hadoop.project
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import scala.io.Source

object enrichedMain extends Preparation with App {

  val routes: FSDataInputStream = fs.open(
    new Path("/user/bdss2001/vish1/stm/routes.txt"))
  val routeMap: Map[Int, Route] = Source.fromInputStream(routes).getLines()
    .toList.tail
    .map(_.split(",", -1))
    .map(rout => rout(0).toInt -> Route(rout(0).toInt, rout(3), rout(6))).toMap
  routes.close()

  def lookup(routeId: Int): Route = routeMap.getOrElse(routeId, null)

  val calendar: FSDataInputStream = fs.open(
    new Path("/user/bdss2001/vish1/stm/calendar.txt"))
  val calendarMap: Map[String, Calendar] = Source.fromInputStream(calendar).getLines()
    .toList.tail
    .map(_.split(",", -1))
    .map(cal => cal(0) -> Calendar(cal(0), cal(8), cal(9))).toMap
  calendar.close()



  def lookup(serviceId: String): Calendar = calendarMap.getOrElse(serviceId, null)



  val trips: FSDataInputStream = fs.open(
    new Path("/user/bdss2001/vish1/stm/trips.txt"))
  val tripMap = Source.fromInputStream(trips).getLines().toList.tail.map(_.split(",", -1))
    .map(tri => Trip(tri(0).toInt, tri(1), tri(2), tri(3), tri(6).toInt))

    .map(trip => TripRoute(trip, lookup(trip.routeId)))
    .map(tripRoute => EnrichedTrip(tripRoute, lookup(tripRoute.trip.serviceId)))

  val finalOutput = tripMap.map(output => {
    val t = Trip.toCsv(output.tripRoute.trip)
    val r = Route.toCsv(output.tripRoute.route)
    val c = Calendar.toCsv(output.calendar)

    t + "," + r + "," + c
  })

  val header: String = "route_id,service_id,trip_id,tripHead,wheelchair_accessible," +
    "route_id,route_long_name,route_color," +
    "service_id,start_date,end_date"

  try {
    val filePath = new Path("/user/bdss2001/vish1/course3")
    if (fs.exists(filePath)) {
      val deleteResult = fs.delete(new Path("/user/bdss2001/vish1/course3"), true)
      println(deleteResult, "file exists, delete directory")

    }
    else {
      println("file not exists,creating directory")
      fs.mkdirs(filePath)
    }

  val fileOutput: FSDataOutputStream = fs.create(new Path("/user/bdss2001/vish1/course3/final_output.csv"))

  fileOutput.writeChars(header)
  for (i <- finalOutput) {

    fileOutput.writeChars("\n")
    fileOutput.writeChars(i)

  }
  trips.close()
  fileOutput.close()
  fs.close()

  }

}
