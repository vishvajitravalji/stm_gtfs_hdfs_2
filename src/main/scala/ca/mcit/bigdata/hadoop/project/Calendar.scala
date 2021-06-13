package ca.mcit.bigdata.hadoop.project

case class Calendar(
                     service_id:String,
                     startDate: String,
                     endDate: String
                   )
object Calendar {
  def toCsv(calendar: Calendar): String = {
      calendar.service_id+ "," +
      calendar.startDate + "," +
      calendar.endDate
  }
}

