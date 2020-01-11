/******************************************************************************
  * Project: datAcron (http://ai-group.ds.unipi.gr/datacron/)
  * Task: 2.1 Trajectory detection & summarization
  * Module: Synopses Generator
  * File: eu.datacron.synopses.maritime/StreamOriginalRateSimulator.scala
  * Description: Simulates a streaming behaviour by consuming kinematic AIS messages from a delimited CSV file, assuming that all messages are in chronological order.
  *              CAUTION! Items are replayed according to their ORIGINAL stream rate (tuples/sec). All messages are assumed chronologically ordered in simulation.
  *              If SLEEP flag is set (sleep=1), replay imitates the exact reporting frequency of messages (i.e., waiting for the next); otherwise, no wait period is applied.
  *              Consumed items (positions) are written into a specific topic of a Kafka stream, conforming to an AVRO attribute schema (file: critical_point.avsc).
  * Developer: Kostas Patroumpas (UPRC)
  * Created: 1/11/2016
  * Revised: 6/6/2017
  ************************************************************************/

package eu.datacron.synopses.maritime

import java.time.Instant
import java.util.Properties

import scala.util.Try
import scala.io.Source
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


object StreamOriginalRateSimulator extends App {

  val sleep: Boolean = Try(if (args(0) == "1" || args(0) == "true") true else false).getOrElse(false)    //SLEEP flag: 0:false; 1:true

  //Read parameter values from configuration properties file
  //OPTION #1: File must be specified under project resources in order to be recognized by Flink at runtime!
  var configProperties : Properties = null

  val url = getClass.getClassLoader.getResource("maritime_config.properties")
  if (url != null) {
    val source = Source.fromURL(url)

    configProperties = new Properties()
    configProperties.load(source.bufferedReader())
  }

  //OPTION #2: File must be specified as an absolute path in order to be recognized by Flink at runtime!
  //val configProperties = new Properties()
  //configProperties.load(new FileInputStream("/opt/datacron/config/trajectory_synopses/maritime_config.properties"))    //Fixed file path

  //Kafka properties
  val props = new Properties()
  props.put("metadata.broker.list", configProperties.getProperty("bootstrap.servers"))              //Kafka broker
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  val topic = configProperties.getProperty("topic_messages")        //Kafka topic for incoming messages
  
  //Configuration read from user-specified properties
  val (delimiter, dataPath, inputFile) =
    try {
      (configProperties.getProperty("delimiter"),
       configProperties.getProperty("dataPath"),
       configProperties.getProperty("inputFile")
      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }

  val filePath = dataPath + inputFile        //Absolute path to input dataset as specified in configuration, e.g., "/opt/datacron/data/input/streaming/locations.csv"

  //Parameters to control the rate in the streaming data
  //CAUTION: All timestamp values in the input file must be expressed in MILLISCECONDS (epochs)!
  var totalMsg:Int = 0
  var cntMsg:Int = 0
  var clock:Long = System.currentTimeMillis()
  var t0:Long = 0
  var ts:Long = 0
  var waitTime:Long = 0

  //Stream out each message in the input file
  for (msg <- Source.fromFile(filePath).getLines()) 
  {
    //Cast message into an object according to AVRO data type for locations (actually critical points)
    val loc:critical_point = parseLocation(msg)
    ts = loc.getTimestamp                        //Timestamp in UNIX epochs (MILLISECONDS)

    //Initialize timestamp value
    if (t0 == 0) {
      t0 = ts
    }

    //Regulate the amount of messages sent per second (i.e., imitate stream arrival rate)
    if (ts > t0) {
      if (sleep == true)   {      //User parameter to activate simulation with original time delay between messages
         waitTime = clock + ts - t0 - System.currentTimeMillis()
      }
      if (waitTime > 0) {
        Thread.sleep(waitTime)
      }

      clock = System.currentTimeMillis()
      t0 = ts
      cntMsg = 0
    }

    //Emit this message as a CSV string into the Kafka stream
    val data = new KeyedMessage[String, String](topic, loc.getId.toString, msg)    //Use object identifier as the partitioning key
    producer.send(data)
    cntMsg += 1
    totalMsg += 1

    //Notify progress...
    if (totalMsg % 1000 == 0) {
      System.out.println(Instant.ofEpochMilli(System.currentTimeMillis()) + " Relayed " + totalMsg + " messages so far...")
    }
  }

  //Termination
  producer.close()
  System.out.println(Instant.ofEpochMilli(System.currentTimeMillis()) + " Input stream simulation completed. " + totalMsg + " AIS messages relayed in total.")

  //*************************************************************************
  //  CUSTOM FUNCTIONS
  //*************************************************************************

  //Extract constituent values from the AIS message: timestamp, id, lon, lat, error flags -- the rest are assigned NULL or zero values (will be calculated during trajectory detection & summarization)
  def parseLocation(line : String): (critical_point) = {
    val tokens = line.substring(0, line.length).split(delimiter)
    new critical_point(tokens(0).toLong, tokens(1).toString, tokens(2).toDouble, tokens(3).toDouble, null, 0.0F, 0.0F, 0.0F, 0L, tokens(6).toString, 0L)
  }

}


