package ca.mcit.bigdata.hadoop.project

case class Trip(
                 routeId: Int,
                 serviceId: String,
                 tripId: String,
                 tripHead: String,
                 wheelchairAccessible: Int
               )
object Trip {
  def toCsv(trip: Trip): String = {
    trip.routeId + "," +
      trip.serviceId + "," +
      trip.tripId + "," +
      trip.tripHead + "," +
      trip.wheelchairAccessible
  }
}
