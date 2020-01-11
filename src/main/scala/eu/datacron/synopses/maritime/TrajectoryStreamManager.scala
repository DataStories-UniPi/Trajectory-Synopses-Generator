/******************************************************************************
  * Project: datAcron (http://ai-group.ds.unipi.gr/datacron/)
  * Task: 2.1 Trajectory detection & summarization
  * Module: Synopses Generator
  * File: eu.datacron.synopses.maritime/TrajectoryStreamManager.scala
  * Description: Management of kinematic AIS messages in order to detect critical points along trajectories upon changes in each object's mobility features (stop, turn, communication gap, etc.).
  *              Input and output streams adhere to specific AVRO attribute schema (critical_point.avsc)
  * TODO: Improve noise elimination; fine-tune parametrization.
  * Developer: Kostas Patroumpas (UPRC)
  * Created: 31/10/2016
  * Revised: 10/7/2017
  ************************************************************************/

package eu.datacron.synopses.maritime

import eu.datacron.synopses.Config
import java.util.Properties
import java.time.Instant

import scala.io.Source
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.flink.api.common.ExecutionMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.{FetcherType, OffsetStore}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import org.apache.flink.api.common.accumulators.AverageAccumulator
import org.apache.flink.api.common.accumulators.Histogram

object TrajectoryStreamManager {

  //*************************************************************************
  //  LOCAL VARIABLES
  //*************************************************************************

  //Configuration settings
  //OPTION #1: File must be specified under project resources in order to be recognized by Flink at runtime!
  //It also includes parameters used in trajectory compression
  var configProperties : Properties = null

  val url = getClass.getClassLoader.getResource("maritime_config.properties")
  if (url != null) {
    val source = Source.fromURL(url)

    configProperties = new Properties()
    configProperties.load(source.bufferedReader())
  }

  //Read parameter values from configuration properties file
  //OPTION #2: File must be specified as an absolute path in order to be recognized by Flink at runtime!
  //val configProperties = "/opt/datacron/config/trajectory_synopses/maritime_config.properties"

  //Prepare a configuration setting for use with parameters read from the properties file
  //This also includes the bounding box for the area of monitoring
  var config = new Config()
  config = config.fromProperties(configProperties)    //OPTION #1: from a properties array already loaded
  //config = config.fromFile(configProperties)        //OPTION #2: directly from a properties file

  //Kafka properties for the Notifications output stream
  val props = new Properties()
  props.put("metadata.broker.list", config.BOOTSTRAP_SERVERS)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")


  //OUTPUT #3: Custom Kafka producer for notifications
  //Derived (Kafka) stream; essentially, locations with GAP_START annotations emitted with delay
  val configNotifications = new ProducerConfig(props)
  val kafkaProducer_notifications = new Producer[String, String](configNotifications)


  //Auxiliary data structure for maintaining state as a queue of recent LOCATIONS per object in order to calculate its velocity vector
  //Size of this queue is controlled by user-specified parameter val_BUFFER_SIZE
  //CAUTION: Only using the schema of critical points for lightweight maintenance, NOT the actually detected critical points
  var objStates = objStateType()          //In this map structure, Key is the object identifier, Value is a queue of its most recent raw LOCATIONS



  def main(args: Array[String]) {

    //Employ the Flink Streaming API
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setBufferTimeout(100)                                         //To control throughput and latency, set a maximum wait time (in MILLISECONDS) for the buffers to fill up
    env.getConfig.setExecutionMode(ExecutionMode.PIPELINED)           //Data exchanges to be performed in a pipelined manner
    env.getConfig.disableSysoutLogging()                              //JobManager status updates not to be printed to System.out by default
    //env.setParallelism(1)                                             //IMPORTANT!: NO parallelism along the operator chain in order to have consistent results per trajectory
    //env.getConfig.enableObjectReuse()              //DANGEROUS! DO NOT APPLY! Object reuse mode for better performance, but loses items and results!


    System.out.println("PARAMETERS: " + config.toString())

    var lastInputTimestamp = 0L                //Latest timestamp value (i.e., highest value seen thus far) in the input stream
    var lastCleanupTimestamp = 0L              //Latest timestamp value in the input stream at which a cleanup operation was triggered
	
    val lag = config.LAG * 1000L                    //Lag (value internally used in MILLISECONDS) to wait for delayed tuples

    //INPUT: Custom Kafka consumer
    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.setProperty("bootstrap.servers", config.BOOTSTRAP_SERVERS)
    kafkaConsumerProperties.setProperty("zookeeper.connect", config.ZOOKEEPER_CONNECT)
    kafkaConsumerProperties.setProperty("group.id", config.GROUP_ID)
    kafkaConsumerProperties.setProperty("auto.offset.reset", "earliest")      // always read the Kafka topic from start

    //INPUT Kafka consumer
    val kafkaConsumer_Messages = new FlinkKafkaConsumer[String](
      config.TOPIC_MESSAGES,
      new SimpleStringSchema(),                         //Read input messages as CSV strings; currently, NOT pertaining to the AVRO schema of critical points
      kafkaConsumerProperties,
      OffsetStore.FLINK_ZOOKEEPER,
      FetcherType.LEGACY_LOW_LEVEL
    )

    //OUTPUT: Custom Kafka producers:
    //OUTPUT #1: Noise-free positions along trajectories
    val kafkaProducer_Locations : FlinkKafkaProducer[critical_point] = new FlinkKafkaProducer[critical_point](
      config.BOOTSTRAP_SERVERS,
      config.TOPIC_LOCATIONS,
      CriticalPointSchema                          //CAUTION! Currently pertaining to the AVRO schema of resulting critical points
    )

    //OUTPUT #2: Critical points --> trajectory synopsis
    val kafkaProducer_CriticalPoints :FlinkKafkaProducer[critical_point] = new FlinkKafkaProducer[critical_point](
      config.BOOTSTRAP_SERVERS,
      config.TOPIC_CRITICAL_POINTS,
      CriticalPointSchema                          //Pertaining to the AVRO schema of resulting critical points
    )


    //*************************************************************************
    //  STREAM PROCESSING
    //*************************************************************************



    //Data source: messages
    val incomingMessages: DataStream[String] = env.addSource(kafkaConsumer_Messages)

    //Filtering incoming raw positions by the area of monitoring
    //Also, maintain object states; cleanup obsolete states that haven't received updates over a recent time period (value set by config.PARAMS.HISTORY_PERIOD)
    val positionalStream: DataStream[critical_point] = incomingMessages
      .map (r => parseLocation(r))                 //Transform message into a (candidate) critical point
      .filter(new FilterFunction[critical_point]() {
      @throws[Exception]
      def filter(pos: critical_point): Boolean = {
        val anno = new critical_point_annotation()           //Initialize annotation for this potentially critical point
        pos.setAnnotation(anno)
        if (config.BBOX.contains(pos.getLongitude, pos.getLatitude)) {         //Only keep raw positions within the specified area of monitoring
          if (pos.getTimestamp > lastInputTimestamp) {
            lastInputTimestamp = pos.getTimestamp
            //Check if a cleanup operation is needed for the state maintained per object
            if (lastInputTimestamp - lastCleanupTimestamp > config.PARAMS.HISTORY_PERIOD * 1000L) {
              for (vec <- objStates)  {          //Applied to all objects regardless
                //If the latest item in this object's state is obsolete, then the state must be discarded
                if ((vec._2.objHistory.length > 0) && (vec._2.objHistory.last.getTimestamp < (lastInputTimestamp - config.PARAMS.HISTORY_PERIOD * 1000L))) {
                  vec._2.purgeState()
                  objStates.remove(vec._1)                //Remove any reference for this object in the state
                }
              }
              lastCleanupTimestamp = lastInputTimestamp              //Timestamp to be used for triggering the next cleanup of object states
            }
          }
          if (!objStates.contains(pos.getId)) {                   //This is the first location after a communication GAP, ...
            pos.getAnnotation.setGapEnd(true)                      //..., so mark it appropriately
            objStates += (pos.getId -> new ObjectState())
            objStates(pos.getId).initState(pos, config.PARAMS.HISTORY_PERIOD, config.PARAMS.BUFFER_SIZE) //Queue of recent locations is initialized with the current one
          }
          return true
        }
        return false             //This is NOT a valid location; discard it
      }
    }).setParallelism(1)


    //Basic calculations between successive locations of the same object
    //CAUTION: Two count-based windows are applied per object, one for the FORWARD check, and another for the BACKWARD check

    //STEP #1 (FORWARD check): Determine whether the current location should be characterized as a critical point (except for turning points)
    val criticalPointStream: DataStream[critical_point] = positionalStream
      //.assignAscendingTimestamps(_.getTimestamp)
      //Custom timestamp extractor; watermarks allow delayed items according to the LAG parameter
      .assignTimestamps(new TimestampExtractor[(critical_point)] {
      override def getCurrentWatermark: Long = Long.MinValue
      override def extractWatermark(element: critical_point, currentTimestamp: Long): Long = element.getTimestamp - lag
      override def extractTimestamp(element: critical_point, currentTimestamp: Long): Long = element.getTimestamp
    })
      //      val keyStream: KeyedStream[critical_point, CharSequence] = criticalPointStream.keyBy(_.getId)
      .keyBy(_.getId)
      .countWindow(2, 1)            //Window: use the latest pair of raw locations reported per object
      .reduce { (prevLoc: critical_point, curLoc: critical_point) => forwardCheckMobilityFeatures(prevLoc, curLoc) }   //FORWARD check: characterizes the CURRENT location
      .setParallelism(1)

      //STEP #2 (BACKWARD check): Determine whether the previous location should also be characterized as a turning point (where significant change in heading is observed)
      val turningPointStream: DataStream[critical_point] = criticalPointStream
      //.assignAscendingTimestamps(_.getTimestamp)
      //Custom timestamp extractor; watermarks allow delayed items according to the LAG parameter
      .assignTimestamps(new TimestampExtractor[(critical_point)] {
      override def getCurrentWatermark: Long = Long.MinValue
      override def extractWatermark(element: critical_point, currentTimestamp: Long): Long = element.getTimestamp - lag
      override def extractTimestamp(element: critical_point, currentTimestamp: Long): Long = element.getTimestamp
    }).setParallelism(1)
      .keyBy(_.getId)
      .countWindow(2, 1)           //Window: use the latest pair of raw locations reported per object
      .reduce { (prevLoc: critical_point, curLoc: critical_point) => backwardCheckMobilityFeatures(prevLoc, curLoc) }      //BACKWARD check: characterizes the PREVIOUS location
      .setParallelism(1)


    //DERIVED OUTPUT #1: Noise-free locations detected along this trajectory
    turningPointStream
      .filter(_.getAnnotation.getNoise == false)
      .addSink(kafkaProducer_Locations)


    //Maintain a LOG file with all original messages (including those qualified as noise)
    if (config.LOG_FILE != "") {                //Activate logging, unless no file has been specified in application configuration
      turningPointStream
        .rebalance                              //In a streaming context, this is necessary in order to break the operator chain
        .writeAsText(config.LOG_FILE, 30000L).setParallelism(1) //Update a single LOG file every 30 seconds
    }


    //DERIVED OUTPUT #2: Filter out non-critical points from the trajectory synopsis
    turningPointStream.filter(new FilterFunction[critical_point]() {
      @throws[Exception]
      def filter(pos: critical_point): Boolean = {
        val anno = pos.getAnnotation
        if ((pos.getTimeElapsed > 0) && (anno.getNoise != true)) {       //Examine non-noisy positions; condition with elapsed time is used to filter out the first ever point in the window (which it duplicates itself)
          for (i <- 0 until (anno.getSchema.getFields.size() - 1)) {     //Check annotation flags except for the last one (signifying NOISE)
            if (anno.get(i) == true) {
              return true    //At least one flag is set, so emit this location as a critical point
            }
          }
        }
        return false             //This is NOT a critical point
      }
    })
      .addSink(kafkaProducer_CriticalPoints)      //OUTPUT resulting critical points as a Kafka stream at that specific topic


    val jobResult = env.execute("Maritime Trajectory Stream Manager")
    //System.out.println("Job completed in " + jobResult.getNetRuntime + " milliseconds" )

  }


  //*************************************************************************
  //  CUSTOM FUNCTIONS for noise elimination and trajectory summarization
  //*************************************************************************

  //Extract constituent values from the AIS message: timestamp, id, lon, lat, error flags -- the rest are assigned NULL or zero values (will be calculated during trajectory detection & summarization)
  //Also insert an ingestion timestamp (in MILLISECONDS), so as to measure tuple processing latency across the operator pipeline
  def parseLocation(msg : String): (critical_point) = {
    val tokens = msg.substring(0, msg.length).split(config.DELIMITER)
    //System.out.println("HASH: " + Math.abs(tokens(1).toString.hashCode()) + " residual: " + Math.abs(tokens(1).toString.hashCode()) % 2)
    new critical_point(tokens(0).toLong, tokens(1).toString, tokens(2).toDouble, tokens(3).toDouble, null, 0.0F, 0.0F, 0.0F, 0L, tokens(6).toString, System.currentTimeMillis())
  }




  //Apply noise filtering to incoming location (considered as a candidate critical point) w.r.t. to the previously reported one
  //Detect noise and accordingly update the object state (actually the history of recent locations maintained for this particular object)
  def eliminateNoise(oldLoc: critical_point, newLoc: critical_point): Boolean = {

    var isNoise = false                    //Flag: no location qualifies as noise beforehand

    //Purge previous state altogether and insert the new location only
    if (newLoc.getAnnotation.getGapEnd == true) {
      objStates(newLoc.getId).purgeState()          //Invalidate previous state in case of a communication gap
      objStates(newLoc.getId).restoreState(newLoc)  //Queue of recent raw locations is initialized with the current one
      return false                                  //With only a single location available, no kind of noise can be possibly determined
    }

    //First, in case of NULL or EXCESSIVE speed, this location qualifies for noise
    if ((newLoc.getSpeed < 0.0F) || (newLoc.getSpeed >= config.PARAMS.MAX_SPEED_THRESHOLD)) {
      isNoise = true
    }
    //Also check whether there has been any improbable change of rate in instantaneous speed (i.e., huge acceleration or deceleration)
    else if (Math.abs(MobilityChecker.getRateOfChangeKnots(oldLoc, newLoc)) >= config.PARAMS.MAX_RATE_OF_CHANGE) {
      isNoise = true                      //Mark this location as noise
    }
    //Next, check if there has been a sudden surge in the rate of turn, provided that the object is NOT stopped (i.e., agility during stop should NOT be considered as noise)
    else if ((newLoc.getSpeed > config.PARAMS.LOW_SPEED_THRESHOLD) && (MobilityChecker.getRateOfTurn(newLoc, oldLoc) >= config.PARAMS.MAX_RATE_OF_TURN)) {
      isNoise = true                     //Mark this location as noise
    }

    isNoise            //Return flag: whether this location qualifies as noise or not
  }


  //BACKWARD check: Detect any significant change in heading between two consecutive locations
  //... and characterize accordingly the OLDEST location, because at that point the change in course actually took place
  def backwardCheckMobilityFeatures(oldLoc: critical_point, newLoc: critical_point): critical_point = {

    val curState = objStates(newLoc.getId)

    //IMPORTANT: Check for changes in heading as long as this object is NOT marked as stopped
    if (!curState.isStopped) {

      //Change of heading above threshold w.r.t. previous heading --> CHANGE_IN_HEADING
      //Compare also with the mean heading over the recent motion history of this object
      if ((MobilityChecker.angleDifference(newLoc.getHeading, curState.getMeanHeading) > config.PARAMS.ANGLE_THRESHOLD) || (Math.abs(curState.getCumulativeHeading) > config.PARAMS.ANGLE_THRESHOLD)) {
        oldLoc.getAnnotation.setChangeInHeading(true)
        objStates(newLoc.getId).cleanupState()             //Since the object changed its heading, removed older items except for the last two ones
      }
    }

    oldLoc      //Return annotated the first (oldest) one of this pair of consecutive locations
  }


  //FORWARD check: Calculate spatiotemporal measures from pairs of consecutive locations per object
  //... and determine suitable annotations for the latest critical point
  def forwardCheckMobilityFeatures(prevLoc: critical_point, newLoc: critical_point): critical_point = {

    var oldLoc: critical_point =  null

    //In case the PREVIOUS location had been marked as noise, then computation of the instantaneous spatiotemporal features for the CURRENT location ...
    //... must bypass the previous one and use the last location available in the tail of the respective queue
    if ((prevLoc.getAnnotation.getNoise == true) && (objStates(newLoc.getId).objHistory.length > 0))
      oldLoc = objStates(newLoc.getId).objHistory.last     //CAUTION: At this point, the queue should contain at least one buffered item
    else
      oldLoc = prevLoc                                      //Previously reported location is not noisy and it can be safely used for computations

    //Compute instantaneous spatiotemporal features between the two locations
    newLoc.setDistance(MobilityChecker.getHaversineDistance(oldLoc, newLoc))
    newLoc.setHeading(MobilityChecker.getBearing(oldLoc, newLoc))
    newLoc.setTimeElapsed(newLoc.getTimestamp - oldLoc.getTimestamp)                            //Time elapsed since previously reported (non-noisy) location

    //Delayed locations are automatically characterized as noise
    if (newLoc.getTimeElapsed <= 0L) {
      newLoc.getAnnotation.setNoise(true)
      return newLoc                         //Any further processing is meaningless
    }

    //Instantaneous speed
    newLoc.setSpeed({
      if (newLoc.getTimeElapsed > 0L)                                             //timestamps in milliseconds
        (3600000.0D * newLoc.getDistance) / (1852.0D * newLoc.getTimeElapsed)     //Get value in knots
      else
        -1.0D            //Placeholder for NULL speed
    })

    //Instantaneous heading
    newLoc.setHeading({
        MobilityChecker.getBearing(oldLoc, newLoc)
    })


    //Communication has been restored after a time period --> mark GAP_END to this critical point and also emit a GAP_START notification
    if (newLoc.getTimeElapsed > 1000.0F * config.PARAMS.GAP_PERIOD) {

      newLoc.getAnnotation.setGapEnd(true)

      //IMPORTANT: The previously reported location must be marked as GAP_START by pushing a message to appropriate Kafka topic
      oldLoc.getAnnotation.setGapStart(true)
      val msgNotify = new KeyedMessage[String, String](config.TOPIC_NOTIFICATIONS, oldLoc.getTimestamp.toString, oldLoc.toString)    //Use timestamp as the partitioning key
      kafkaProducer_notifications.send(msgNotify)

      //Since this is the first position reported after a long time, no further processing is possible
      objStates(newLoc.getId).purgeState()
      objStates(newLoc.getId).updateState(newLoc)         //New state should hold this position only

      return newLoc
    }

    //Apply filtering w.r.t. NOISE before any further processing
    newLoc.getAnnotation.setNoise(eliminateNoise(oldLoc, newLoc))

    //Append new location to the state of this object, UNLESS it has just been marked as noise
    if (newLoc.getAnnotation.getNoise == true) {
      return newLoc                            //Any further processing is meaningless
    }
    else {
      objStates(newLoc.getId).updateState(newLoc)    //This is correct; object state already exists
    }


    val curState = objStates(newLoc.getId)                         //Velocity vector with the recent motion history of this object

    //If less than two past locations are held in motion history (apparently, only the current one), then no calculations can be made
    if (curState.objHistory.length < 2) {
      return newLoc
    }

    //CAUTION! Both check conditions concerning STOP cannot hold simultaneously!
    if ((newLoc.getSpeed < config.PARAMS.NO_SPEED_THRESHOLD) && !curState.isStopped) {
      newLoc.getAnnotation.setStopStart(true)
      objStates(newLoc.getId).setStopStatus(true)

      //Once a stop has started, terminate any previous slow motion phenomenon
      if (curState.inSlowMotion) {
        newLoc.getAnnotation.setSlowMotionEnd(true)
        objStates(newLoc.getId).setSlowMotionStatus(false)
      }
      //Once a stop has started, terminate any previous change in speed phenomenon
      if (curState.hasChangedSpeed) {
        newLoc.getAnnotation.setChangeInSpeedEnd(true)
        objStates(newLoc.getId).setSpeedStatus(false)
      }
    }
    //Either criterion holds: no speed or distance threshold --> STOP END
    else if (((newLoc.getSpeed >= config.PARAMS.NO_SPEED_THRESHOLD) || (newLoc.getDistance >= config.PARAMS.DISTANCE_THRESHOLD)) && curState.isStopped) {
      newLoc.getAnnotation.setStopEnd(true)
      objStates(newLoc.getId).setStopStatus(false)
    }


    //IMPORTANT: Check for any other mobility features as long as this object is NOT marked as stopped
    if (!curState.isStopped) {

      //Speed ratio threshold --> CHANGE_IN_SPEED_START
      val mean_speed = curState.getMeanSpeed
      if ((Math.abs((newLoc.getSpeed - mean_speed) / mean_speed) > config.PARAMS.SPEED_RATIO) && !curState.hasChangedSpeed) {
        newLoc.getAnnotation.setChangeInSpeedStart(true)
        objStates(newLoc.getId).setSpeedStatus(true)
      }

      //Speed ratio threshold --> CHANGE_IN_SPEED_END
      if ((Math.abs((newLoc.getSpeed - mean_speed) / mean_speed) <= config.PARAMS.SPEED_RATIO) && curState.hasChangedSpeed) {
        newLoc.getAnnotation.setChangeInSpeedEnd(true)
        objStates(newLoc.getId).setSpeedStatus(false)
      }

      //Low speed threshold --> SLOW_MOTION_START
      if ((newLoc.getSpeed <= config.PARAMS.LOW_SPEED_THRESHOLD) && (oldLoc.getSpeed > config.PARAMS.LOW_SPEED_THRESHOLD) && !curState.inSlowMotion) {
        newLoc.getAnnotation.setSlowMotionStart(true)
        objStates(newLoc.getId).setSlowMotionStatus(true)
      }

      //Low speed threshold or the object just stopped--> SLOW_MOTION_END
      if ((newLoc.getSpeed > config.PARAMS.LOW_SPEED_THRESHOLD) && (oldLoc.getSpeed <= config.PARAMS.LOW_SPEED_THRESHOLD) && curState.inSlowMotion) {
        newLoc.getAnnotation.setSlowMotionEnd(true)
        objStates(newLoc.getId).setSlowMotionStatus(false)
      }

    }

    newLoc             //Return critical point with updated annotation
  }

}