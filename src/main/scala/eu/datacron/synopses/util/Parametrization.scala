/******************************************************************************
  * Project: datAcron
  * File: eu.datacron.synopses.util/Parametrization.scala
  * Description: Holds parameters used in maintenance of trajectory synopses.
  * Developer: Kostas Patroumpas (UPRC)
  * Date: 20/12/2016
  * Revised: 3/6/2017
  ************************************************************************/

package eu.datacron.synopses

import java.util.Properties
import org.apache.flink.api.java.utils.ParameterTool

/**
  * Configuration defines basic parameters for use during trajectory summarization, i.e., for detection and characterization of critical points along each trajectory.
  *
  * @param DISTANCE_THRESHOLD => 5.0F        //meters
  * @param ANGLE_THRESHOLD => 5.0F           //degrees
  * @param GAP_PERIOD => 600L                //seconds
  * @param HISTORY_PERIOD => 1000L           //seconds
  * @param BUFFER_SIZE => 5                  //number of most recent raw point locations to be used in velocity vector computations
  * @param ANGLE_NOISE_THRESHOLD => 120.0F   //turn more than this angle should be considered as noise
  * @param CLIMB_THRESHOLD => 5.0F           //feet per second
  * @param CLIMB_NOISE_THRESHOLD => 100.0F   //feet per second
  * @param SPEED_RATIO => 0.25F              //percentage: acceleration or deceleration by more than 25%
  * @param NO_SPEED_THRESHOLD => 1.0F        //knots (1 knot = 1.852 kmh)
  * @param LOW_SPEED_THRESHOLD => 1.0F       //knots (1 knot = 1.852 kmh)
  * @param MAX_SPEED_THRESHOLD => 30.0F      //knots (1 knot = 1.852 kmh)
  * @param MAX_RATE_OF_CHANGE => 100.0F      //knots per hour
  * @param MAX_RATE_OF_TURN => 3.0F          //degrees (azimuth) per second
  * @param MAX_ALTITUDE_STOP => 200.0F       //feet
  **/

case class Parametrization(
      var DISTANCE_THRESHOLD: Double,           //meters
      var ANGLE_THRESHOLD: Double,              //degrees
      var GAP_PERIOD: Long,                     //seconds (UNIX epochs)
      var HISTORY_PERIOD: Long,                 //seconds (UNIX epochs): period for which older positions will be held for velocity vector computations
      var BUFFER_SIZE: Int,                     //number of most recent point locations to be used in velocity vector calculation
      var ANGLE_NOISE_THRESHOLD: Double,        //degrees
      var CLIMB_THRESHOLD: Double,              //feet per second
      var CLIMB_NOISE_THRESHOLD: Double,        //feet per second
      var SPEED_RATIO: Double,                  //percentage (%): acceleration or deceleration by more than SPEED_RATIO between two successive locations
      var NO_SPEED_THRESHOLD: Double,           //threshold in knots (1 knot = 1.852 kmh)
      var LOW_SPEED_THRESHOLD: Double,          //threshold in knots (1 knot = 1.852 kmh)
      var MAX_SPEED_THRESHOLD: Double,          //threshold in knots (1 knot = 1.852 kmh)
      var MAX_RATE_OF_CHANGE: Double,           //knots per hour
      var MAX_RATE_OF_TURN: Double,             //degrees (azimuth) per second
      var MAX_ALTITUDE_STOP: Double)            //feet
{


  //Constructor
  def this() {
    this(5.0F, 5.0F, 600L, 1000L, 5, 120.0F, 5.0F, 200.0F, 0.25F, 1.0F, 1.0F, 30.0F, 100.0F, 3.0F, 200.0F)        //DEFAULT values
  }


  //Print out current properties
  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(DISTANCE_THRESHOLD).append(" | ")
    sb.append(ANGLE_THRESHOLD).append(" | ")
    sb.append(GAP_PERIOD).append(" | ")
    sb.append(HISTORY_PERIOD).append(" | ")
    sb.append(BUFFER_SIZE).append(" | ")
    sb.append(ANGLE_NOISE_THRESHOLD).append(" | ")
    sb.append(CLIMB_THRESHOLD).append(" | ")
    sb.append(CLIMB_NOISE_THRESHOLD).append(" | ")
    sb.append(SPEED_RATIO).append(" | ")
    sb.append(NO_SPEED_THRESHOLD).append(" | ")
    sb.append(LOW_SPEED_THRESHOLD).append(" | ")
    sb.append(MAX_SPEED_THRESHOLD).append(" | ")
    sb.append(MAX_RATE_OF_CHANGE).append(" | ")
    sb.append(MAX_RATE_OF_TURN).append(" | ")
    sb.append(MAX_ALTITUDE_STOP)
    sb.toString()
  }


  //Apply a configuration from settings stored in a properties file
  def fromFile(propertiesFile: String): Parametrization = {

    try {
      val parameters: ParameterTool = ParameterTool.fromPropertiesFile(propertiesFile)
      this.DISTANCE_THRESHOLD = parameters.get("val_DISTANCE_THRESHOLD").toDouble        //meters
      this.ANGLE_THRESHOLD = parameters.get("val_ANGLE_THRESHOLD").toDouble              //degrees
      this.GAP_PERIOD = parameters.get("val_GAP_PERIOD").toLong                          //seconds
      this.HISTORY_PERIOD = parameters.get("val_HISTORY_PERIOD").toLong                  //seconds
      this.BUFFER_SIZE = parameters.get("val_BUFFER_SIZE").toInt                         //number of most recent point locations to be used in velocity vector computations
      this.ANGLE_NOISE_THRESHOLD = parameters.get("val_ANGLE_NOISE_THRESHOLD").toDouble  //degrees
      this.CLIMB_THRESHOLD = parameters.get("val_CLIMB_THRESHOLD").toDouble              //feet per second
      this.CLIMB_NOISE_THRESHOLD = parameters.get("val_CLIMB_NOISE_THRESHOLD").toDouble  //feet per second
      this.SPEED_RATIO = parameters.get("val_SPEED_RATIO").toDouble                      //percentage
      this.NO_SPEED_THRESHOLD = parameters.get("val_NO_SPEED_THRESHOLD").toDouble        //knots
      this.LOW_SPEED_THRESHOLD = parameters.get("val_LOW_SPEED_THRESHOLD").toDouble      //knots
      this.MAX_SPEED_THRESHOLD = parameters.get("val_MAX_SPEED_THRESHOLD").toDouble      //knots
      this.MAX_RATE_OF_CHANGE = parameters.get("val_MAX_RATE_OF_CHANGE").toDouble        //knots per hour
      this.MAX_RATE_OF_TURN = parameters.get("val_MAX_RATE_OF_TURN").toDouble            //degrees (azimuth) per second
      this.MAX_ALTITUDE_STOP = parameters.get("val_MAX_ALTITUDE_STOP").toDouble          //feet

      //System.out.println("Configuration updated!")

      Parametrization(DISTANCE_THRESHOLD, ANGLE_THRESHOLD, GAP_PERIOD, HISTORY_PERIOD, BUFFER_SIZE, ANGLE_NOISE_THRESHOLD, CLIMB_THRESHOLD, CLIMB_NOISE_THRESHOLD, SPEED_RATIO, NO_SPEED_THRESHOLD, LOW_SPEED_THRESHOLD, MAX_SPEED_THRESHOLD, MAX_RATE_OF_CHANGE, MAX_RATE_OF_TURN, MAX_ALTITUDE_STOP)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid parametrization", nfe)
    }
  }

  //Apply a configuration from a properties array
  def fromProperties(parameters: Properties): Parametrization = {

    try {
      this.DISTANCE_THRESHOLD = parameters.getProperty("val_DISTANCE_THRESHOLD").toDouble        //meters
      this.ANGLE_THRESHOLD = parameters.getProperty("val_ANGLE_THRESHOLD").toDouble              //degrees
      this.GAP_PERIOD = parameters.getProperty("val_GAP_PERIOD").toLong                          //seconds
      this.HISTORY_PERIOD = parameters.getProperty("val_HISTORY_PERIOD").toLong                  //seconds
      this.BUFFER_SIZE = parameters.getProperty("val_BUFFER_SIZE").toInt                         //number of most recent point locations to be used in velocity vector computations
      this.ANGLE_NOISE_THRESHOLD = parameters.getProperty("val_ANGLE_NOISE_THRESHOLD").toDouble  //degrees
      this.CLIMB_THRESHOLD = parameters.getProperty("val_CLIMB_THRESHOLD").toDouble              //feet per second
      this.CLIMB_NOISE_THRESHOLD = parameters.getProperty("val_CLIMB_NOISE_THRESHOLD").toDouble  //feet per second
      this.SPEED_RATIO = parameters.getProperty("val_SPEED_RATIO").toDouble                      //percentage
      this.NO_SPEED_THRESHOLD = parameters.getProperty("val_NO_SPEED_THRESHOLD").toDouble        //knots
      this.LOW_SPEED_THRESHOLD = parameters.getProperty("val_LOW_SPEED_THRESHOLD").toDouble      //knots
      this.MAX_SPEED_THRESHOLD = parameters.getProperty("val_MAX_SPEED_THRESHOLD").toDouble      //knots
      this.MAX_RATE_OF_CHANGE = parameters.getProperty("val_MAX_RATE_OF_CHANGE").toDouble        //knots per hour
      this.MAX_RATE_OF_TURN = parameters.getProperty("val_MAX_RATE_OF_TURN").toDouble            //degrees (azimuth) per second
      this.MAX_ALTITUDE_STOP = parameters.getProperty("val_MAX_ALTITUDE_STOP").toDouble          //feet

      //System.out.println("Configuration updated!")

      Parametrization(DISTANCE_THRESHOLD, ANGLE_THRESHOLD, GAP_PERIOD, HISTORY_PERIOD, BUFFER_SIZE, ANGLE_NOISE_THRESHOLD, CLIMB_THRESHOLD, CLIMB_NOISE_THRESHOLD, SPEED_RATIO, NO_SPEED_THRESHOLD, LOW_SPEED_THRESHOLD, MAX_SPEED_THRESHOLD, MAX_RATE_OF_CHANGE, MAX_RATE_OF_TURN, MAX_ALTITUDE_STOP)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid parametrization", nfe)
    }
  }

}
