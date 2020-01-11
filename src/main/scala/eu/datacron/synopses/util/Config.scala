/******************************************************************************
  * Project: datAcron
  * File: eu.datacron.synopses.util/Config.scala
  * Description: Holds configuration parameters as read from a properties file. A Configuration describes basic parameters for maintaining trajectory synopses.
  * Developer: Kostas Patroumpas (UPRC)
  * Date: 20/12/2016
  * Revised: 3/6/2017
  ************************************************************************/

package eu.datacron.synopses


import java.util.Properties
import org.apache.flink.api.java.utils.ParameterTool

/**
  * Configuration describes basic parameters for use during trajectory detection & summarization.
  *
  * @param NUM_NODES => 4
  * @param LOG_FILE => "/opt/datacron/data/output/exection.log"
  * @param DELIMITER => '\u0020'
  * @param BOOTSTRAP_SERVERS => "localhost:9092"
  * @param ZOOKEEPER_CONNECT => "localhost:2181"
  * @param GROUP_ID => "TrajectorySynopses"
  * @param TOPIC_MESSAGES => "messages"
  * @param TOPIC_LOCATIONS => "locations"
  * @param TOPIC_CRITICAL_POINTS => "critical_points"
  * @param TOPIC_NOTIFICATIONS => "notifications"
  * @param LAG => 60L
  * @param BBOX => -12.0;30.0;37.0;52.0
  * @param PARAMS =>                                 //A series of parameters to control trajectory summarization; specified in Parametrization.scala
  **/

case class Config(
      var NUM_NODES: Int,                       //Number of processing nodes in parallel execution
      var LOG_FILE: String,                     //Path & name of the log file where all input messages will be stored with appropriate flags; if left BLANK, then no log file will be generated
      var DELIMITER: Char,                      //Delimiter character for input CSV file
      var BOOTSTRAP_SERVERS: String,            //host:port
      var ZOOKEEPER_CONNECT: String,            //host:port
      var GROUP_ID: String,
      var TOPIC_MESSAGES: String,               //Kafka topics used by the application
      var TOPIC_LOCATIONS: String,
      var TOPIC_CRITICAL_POINTS: String,
      var TOPIC_NOTIFICATIONS: String,
      var LAG: Long,                            //seconds
      var BBOX: BoundingBox,                    //bounding box
      var PARAMS: Parametrization)              //Parameters used for detecting critical points in trajectory synopses 
{


  //Constructor
  def this() {
    this(1, "", '\u0020', "localhost:9092", "localhost:2181", "TrajectorySynopses", "messages", "locations", "critical_points", "notifications", 60L, new BoundingBox(), new Parametrization())        //DEFAULT values
  }


  //Print out current properties
  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(NUM_NODES).append(" | ")
    sb.append(LOG_FILE).append(" | ")
    sb.append(DELIMITER).append(" | ")
    sb.append(BOOTSTRAP_SERVERS).append(" | ")
    sb.append(ZOOKEEPER_CONNECT).append(" | ")
    sb.append(GROUP_ID).append(" | ")
    sb.append(TOPIC_MESSAGES).append(" | ")
    sb.append(TOPIC_LOCATIONS).append(" | ")
    sb.append(TOPIC_CRITICAL_POINTS).append(" | ")
    sb.append(TOPIC_NOTIFICATIONS).append(" | ")
    sb.append(LAG).append(" | ")
    sb.append(BBOX.toString).append(" | ")
    sb.append(PARAMS.toString)
    sb.toString()
  }


  //Apply a configuration from settings stored in a properties file
  def fromFile(propertiesFile: String): Config = {

    try {
      val parameters: ParameterTool = ParameterTool.fromPropertiesFile(propertiesFile)
      this.NUM_NODES = parameters.get("num_nodes").toInt
      if ((parameters.get("logPath").trim != "") && (parameters.get("logFile").trim != "")) {
         this.LOG_FILE = parameters.get("logPath") + parameters.get("logFile")
      }
      else {
        this.LOG_FILE = ""
      }
      this.DELIMITER = parameters.get("delimiter").charAt(0)                            //A single character used as a delimiter between attribute values
      this.BOOTSTRAP_SERVERS = parameters.get("bootstrap.servers")                      //host:port
      this.ZOOKEEPER_CONNECT = parameters.get("zookeeper.connect")                      //host:port
      this.GROUP_ID = parameters.get("group.id")
      this.TOPIC_MESSAGES = parameters.get("topic_messages")
      this.TOPIC_LOCATIONS = parameters.get("topic_locations")
      this.TOPIC_CRITICAL_POINTS = parameters.get("topic_critical_points")
      this.TOPIC_NOTIFICATIONS = parameters.get("topic_notifications")
      this.LAG = parameters.get("val_LAG").toLong                                        //seconds
      this.BBOX = new BoundingBox()
      this.BBOX = this.BBOX.constructBBox(parameters.get("val_BBOX"))
      this.PARAMS = new Parametrization()                                                //all parameters used in maintenance of trajectory synopses
      this.PARAMS = this.PARAMS.fromFile(propertiesFile)

       //System.out.println("Configuration updated!")

      Config(NUM_NODES, LOG_FILE, DELIMITER, BOOTSTRAP_SERVERS, ZOOKEEPER_CONNECT, GROUP_ID, TOPIC_MESSAGES, TOPIC_LOCATIONS, TOPIC_CRITICAL_POINTS, TOPIC_NOTIFICATIONS, LAG, BBOX, PARAMS)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid configuration", nfe)
    }
  }

  //Apply a configuration from a properties array
  def fromProperties(parameters: Properties): Config = {

    try {
      //val parameters: ParameterTool = ParameterTool.fromPropertiesFile(propertiesFile)
      this.NUM_NODES = parameters.getProperty("num_nodes").toInt
      if ((parameters.getProperty("logPath").trim != "") && (parameters.getProperty("logFile").trim != "")) {
        this.LOG_FILE = parameters.get("logPath") + parameters.getProperty("logFile")
      }
      else {
        this.LOG_FILE = ""
      }
      this.DELIMITER = parameters.getProperty("delimiter").charAt(0)                            //A single character used as a delimiter between attribute values
      this.BOOTSTRAP_SERVERS = parameters.getProperty("bootstrap.servers")                      //host:port
      this.ZOOKEEPER_CONNECT = parameters.getProperty("zookeeper.connect")                      //host:port
      this.GROUP_ID = parameters.getProperty("group.id")
      this.TOPIC_MESSAGES = parameters.getProperty("topic_messages")
      this.TOPIC_LOCATIONS = parameters.getProperty("topic_locations")
      this.TOPIC_CRITICAL_POINTS = parameters.getProperty("topic_critical_points")
      this.TOPIC_NOTIFICATIONS = parameters.getProperty("topic_notifications")
      this.LAG = parameters.getProperty("val_LAG").toLong                                        //seconds
      this.BBOX = new BoundingBox()
      this.BBOX = this.BBOX.constructBBox(parameters.getProperty("val_BBOX"))
      this.PARAMS = new Parametrization()                                                //all parameters used in maintenance of trajectory synopses
      this.PARAMS = this.PARAMS.fromProperties(parameters)

      //System.out.println("Configuration updated!")

      Config(NUM_NODES, LOG_FILE, DELIMITER, BOOTSTRAP_SERVERS, ZOOKEEPER_CONNECT, GROUP_ID, TOPIC_MESSAGES, TOPIC_LOCATIONS, TOPIC_CRITICAL_POINTS, TOPIC_NOTIFICATIONS, LAG, BBOX, PARAMS)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid configuration", nfe)
    }
  }

}
