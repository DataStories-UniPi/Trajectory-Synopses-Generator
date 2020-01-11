/******************************************************************************
  * Project: datAcron
  * File: eu.datacron.synopses.util/BoundingBox.scala
  * Description: Implements a Bounding Box (a rectangle in lon/lat coordinates georeferenced in WGS84) that represents the area of monitoring.
  * Developer: Kostas Patroumpas (UPRC)
  * Date: 27/3/2017
  * Revised: 31/5/2017
  ************************************************************************/

package eu.datacron.synopses

/**
  * A BoundingBox specifies the area of monitoring for trajectories of moving objects.
  * Ttypically: the Mediterranean Sea for the maritime use case; the entire Europe for the aviation use case.
  *
  * @param lon_min => -31.266F      //decimal degrees
  * @param lat_min =>  27.6363F     //decimal degrees
  * @param lon_max =>  39.8693F     //decimal degrees
  * @param lat_max =>  81.0088F     //decimal degrees
  **/

class BoundingBox(
      var lon_min: Double,     //Minimum longitude (lower x-coordinate)
      var lat_min: Double,     //Minimum latitude (left y-coordinate)
      var lon_max: Double,     //Maximum longitude (right x-coordinate)
      var lat_max: Double)     //Maximum latitude (upper y-coordinate)
{


  def this() {
       this(-90.0F, -180.0F, 90.0F, 180.0F)        //DEFAULT value: the entire planet
  }
  
  //Verify that this is a valid bounding box
  require(lon_min <= lon_max, s"Bounding box lower left X: ${lon_min} > upper right X: ${lon_max}")
  require(lat_min <= lat_max, s"Bounding box lower left Y: ${lat_min} > upper right Y: ${lat_max}")

  //Check whether a given point location is contained within the bounding box
  def contains(p_lon: Double, p_lat: Double): Boolean = (lon_min <= p_lon && p_lon <= lon_max) &&
    (lat_min <= p_lat && p_lat <= lat_max)

  //Construct the bounding box from the given user-specified string (assuming values are separated by ';')
  def constructBBox(strBBox: String): BoundingBox = {
    try {
	     val bounds = strBBox.split(';')
         new BoundingBox(bounds(0).toDouble, bounds(1).toDouble, bounds(2).toDouble, bounds(3).toDouble)
		 }
	catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid parametrization", nfe)
    }
  }

  //Return a WKT-like representation of this bounding box
  override def toString: String = "BBOX(" + lon_min + " " + lat_min + ", " + lon_max + " " + lat_max + ")"

}

