package com.mcit.bigdata

object EnrichedTrip {
  def formatOutput(trip: Trip, route: Route, calendar: Calendar): String = {
    trip.tripId + "," +
      trip.serviceId + "," +
      trip.tripId+ "," +
      trip.tripHeadSign + "," +
      trip.directionId + "," +
      trip.shapeId + "," +
      trip.wheelchairAccessible + "," +
      trip.noteFr.getOrElse("") + "," +
      trip.noteEn.getOrElse("") + "," +
      route.routeLongName
  }

}
