package ca.mcit.DataEnrichment

import java.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.{BufferedSource, Source}

object DataEnrichment extends App {

  // *************************************************************************************
  //  ***  Setup Hadoop conf
  // *************************************************************************************

  val conf = new Configuration()
  conf.addResource(new Path("/home/marinda/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/marinda/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)

  val myPath: Path = new Path("/user/fall2019/marinda")
  val dataPath: Path = new Path("/user/fall2019/marinda/stm")

  println("URI : "+ fs.getUri)
  println("Working dir : "+ fs.getWorkingDirectory)


  // *************************************************************************************
  //  *** Load data from txt files into scala collections/structures
  //
  //  Create a List from trips.txt
  // *************************************************************************************

  val inStreamTrips = fs.open(new Path(dataPath + "/trips.txt"))
  val tripList: List[Trip] = Iterator
    .continually(inStreamTrips.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Trip(n(0).toInt, n(1), n(2), n(3), n(4).toInt, n(5).toInt, n(6).toInt,
      if (n(7).isEmpty) None else Some(n(7)),
      if (n(8).isEmpty) None else Some(n(8))))
  inStreamTrips.close()

  // Create a List from routes.txt.  Filter for subway services only (route_type =1)

  val inStreamRoutes = fs.open(new Path(dataPath + "/routes.txt"))
  val routeList: List[Route] = Iterator
    .continually(inStreamRoutes.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Route(n(0).toInt, n(1), n(2), n(3), n(4).toInt, n(5), n(6), n(7)))
    .filter(_.routeType == 1)
  inStreamRoutes.close()

  // Create a List from calendar.txt.  Filter for Monday services only

  val inStreamCalendar = fs.open(new Path(dataPath + "/calendar.txt"))
  val calendarList: List[Calendar] = Iterator
    .continually(inStreamCalendar.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Calendar(n(0), n(1).toInt, n(2).toInt, n(3).toInt, n(4).toInt, n(5).toInt, n(6).toInt, n(7).toInt, n(8), n(9)))
    .filter(_.monday == 1)
  inStreamCalendar.close()

  // ***************************************************************************
  //  Enrich the Trip data with Route and Calender info
  //
  //  Join Trips and Routes on the route_ID  using a Map
  // ***************************************************************************

  val routeMap: RouteLookup = new RouteLookup(routeList)
  val routeTrips: List[RouteTrip] =
    tripList.map(line => RouteTrip(line, routeMap.lookup(line.routeId)))
      .filter(_.route != null)

  //
  //  Join routeTrips to Calender using a NestedLoopJoin
  //

  val enrichedTrips: List[JoinOutput] =
    new GenericNestedLoopJoin[RouteTrip, Calendar]((i, j) => i.trip.serviceId == j.serviceId)
      .join(routeTrips, calendarList)

  // ***************************************************************************
  //  Now, in enrichedTrips we have a bit of a messy data structure :
  //  JoinOutput(RouteTrip(Trip(...),Route(...)),Calendar(...))
  //
  //  So we need to un-bundle it into the output format we need
  //    (Note, strictly speaking, because the assignment only asked for the Trip
  //           details to be supplied, we did not need to create such a complex
  //           structure containing all the Route and Calendar info, but as I may
  //           use this code in future, I left it like that,  to keep it more "generic"
  // *************************************************************************************

  val outDataLines: List[String] =
    enrichedTrips
      .map(n =>
        EnrichedTrip.formatOutput(n.left.asInstanceOf[RouteTrip].trip,
          n.left.asInstanceOf[RouteTrip].route,
          n.right.asInstanceOf[Calendar]))

  //  Create the output file

  val outputStream = fs.create(new Path(myPath + "/course3/enrichedtrips.csv"))

  outputStream.writeBytes("route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en,route_long_name")

  for (line <- outDataLines) {
    outputStream.writeBytes("\n" + line)
  }


  outputStream.close()

  println(s"Data enrichment complete.  ${outDataLines.size} records written to file enrichedtrips.csv")

}


