/******************************************************************************
  * Project: datAcron (http://ai-group.ds.unipi.gr/datacron/)
  * Task: 2.1 Trajectory detection & summarization
  * Module: Synopses Generator
  * File: eu.datacron.synopses.maritime/ObjectState.scala
  * Description: Helper class for maintaining the velocity vector and mobility status for a particular moving object (vessel) over a recent time interval.
  * Developer: Kostas Patroumpas (UPRC)
  * Created: 21/3/2017
  * Revised: 9/6/2017
  ************************************************************************/

package eu.datacron.synopses.maritime


//Auxiliary data structure for maintaining state as a queue of recent raw LOCATIONS per object in order to calculate their respective velocity vectors
object objStateType  { def apply() = { scala.collection.mutable.Map[CharSequence, ObjectState]() } }


class ObjectState {

  var id: CharSequence = _;            //Object identifier

  var TIME_SPAN: Long = 0L             //Time interval (in MILLISECONDS) for which positions will be held in queue  (w.r.t. to the latest one)

  var BUFFER_SIZE: Int = 5             //The size of this buffer is controlled by user-specified parameter val_BUFFER_SIZE

  //Maintain a queue of recent, chronologically ordered, noise-free, RAW LOCATIONS per object
  //CAUTION: Only using the schema of critical points for lightweight maintenance of object's state, NOT the actually detected critical points
  var objHistory: scala.collection.mutable.Queue[critical_point] = scala.collection.mutable.Queue[critical_point]()

  //Maintain a bitmap outlining the status of each object
  var objStatus: Array[Boolean] = Array[Boolean]()          //List of three booleans denoting the current status of an object: 0-bit: STOPPED, 1-bit: SPEED_CHANGED, 2-bit: SLOW_MOTION


  //Constructor: initialize state for a given (unique) object identifier
  def initState(curLoc: critical_point, t:Long, n:Int) {

    this.id = curLoc.getId
    this.objHistory.enqueue(curLoc)                 //Push this location into the queue
    this.objStatus = Array(false, false, false)     //Initialize bitmap
    this.TIME_SPAN = t * 1000L                      //Time interval parameter given in SECONDS; but all internal computations are expressed in MILLISECONDS
    this.BUFFER_SIZE = n                            //Max number of items held in this buffer
  }

  //Acts like a destructor: remove all contents from the current state (velocity vector) of this object
  def purgeState() {

    this.objHistory.clear()
    this.objStatus = Array(false, false, false)    //...or should this be set to null?
  }

  //Update the current state (velocity vector) of this object with a new location, and also eliminate obsolete locations from the queue
  def updateState(curLoc: critical_point) {

    this.objHistory.enqueue(curLoc)                 //Push this location into the queue

    //Remove obsolete locations with a timestamp older than a given TIME_SPAN interval (user-specified property in the configuration file)
    while ((this.objHistory.front.getTimestamp < curLoc.getTimestamp - TIME_SPAN) || (this.objHistory.length > BUFFER_SIZE))   {
      this.objHistory.dequeue()
    }
  }

  //Cleanup the current state (velocity vector) of this object, BUT retain the two latest known locations
  def cleanupState() {

    //Remove all locations except the latest two (this is the case after a change in heading)
    while (this.objHistory.length > 2)   {
      this.objHistory.dequeue()
    }
  }

  //In case that previous history has been invalidated, re-instantiate it with the new location
  def restoreState(curLoc: critical_point) {

    this.objHistory.enqueue(curLoc)                 //Push this location into the queue
    this.objStatus = Array(false, false, false)     //Initialize bitmap
  }

  //Check if the given object has a state (i.e., there are past positions stored in its history)
  def hasEmptyState: Boolean = {

    this.objHistory.isEmpty         //Is this queue empty?
  }

  //Check if the given object is stopped
  def isStopped: Boolean = {

    this.objStatus(0) //Return value: First item in the List is used for status STOP
  }

  //Declare this object as stopped
  def setStopStatus(status: Boolean) {

    this.objStatus(0) = status //First item in the List is used for status STOP
  }


  //Check if the given object had changed its speed significantly
  def hasChangedSpeed: Boolean = {

    this.objStatus(1) //Return value: Second item in the List is used for status SPEED_CHANGE
  }

  //Declare this object as having changed its speed
  def setSpeedStatus(status: Boolean) {

    this.objStatus(1) = status //Second item in the List is used for status SPEED_CHANGE
  }


  //Check if the given object is moving slowly
  def inSlowMotion: Boolean = {

    this.objStatus(2)        //Return value: Third item in the List is used for status SLOW_MOTION
  }


  //Declare this object as having changed its speed
  def setSlowMotionStatus(status: Boolean) {

    objStatus(2) = status //Third item in the List is used for status SLOW_MOTION
  }


/*
    //Calculate the mean speed of the given object based on its most recent locations
    //OPTION #1: Take the average of the computed instantaneous speeds
    def getMeanSpeed(id: CharSequence): Double = {

      //Calculate the sum of all speed values contained in the queue
      val sum_speed = objHistory.foldLeft(0.0D)((acc, v) => acc + v.getSpeed)

      sum_speed / (1.0D * objHistory.length)   //Return value
    }
*/

  //Calculate the mean speed of the given object based on its most recent locations
  //OPTION #2: Calculate the quotient of total displacement over the elapsed time
  def getMeanSpeed: Double = {

    //Average speed is calculated from total displacement over the elapsed time
    (3600000.0D * MobilityChecker.getHaversineDistance(objHistory.front, objHistory.last)) / (1852.0D * (objHistory.last.getTimestamp - objHistory.front.getTimestamp))    //Return value in knots
  }

/*
  //Calculate the mean heading of this object based on its most recently reported locations
  //OPTION #1: Calculate the overall change in heading and return the respective angle value
  def getMeanHeading: Double = {

    //Calculate the mean of angles
    val sum_sin = objHistory.foldLeft(0.0D)((acc, v) => acc + Math.sin(v.getHeading))
    val sum_cos = objHistory.foldLeft(0.0D)((acc, v) => acc + Math.cos(v.getHeading))

    Math.atan2(sum_sin, sum_cos)     //Return angle value between 0 and 359 degrees
  }
*/

/*
  //Calculate the mean heading of this object based on its most recently reported locations
  //OPTION #2: Take the average of the computed instantaneous headings
  def getMeanHeading: Double = {

    //Calculate the mean of angles
    val sum_heading = objHistory.foldLeft(0.0D)((acc, v) => acc + v.getHeading)

    sum_heading / (1.0D * objHistory.length)    //Return angle value between 0 and 359 degrees
  }
*/

  //Calculate the mean heading of this object based on its most recently reported locations
  //OPTION #3: Calculate the azimuth angle between the oldest and the one but the last location in this object's state
  def getMeanHeading: Double = {
    val iter = objHistory.iterator
    var last = iter.next()
    if (iter.hasNext) {
      val prev = iter.next()                              //Find out the location received before the last one
      MobilityChecker.getBearing(objHistory.front, prev)  //Return angle value between 0 and 359 degrees
    }
    else
      0.0D           //Placeholder for NULL
  }

  //Calculate the cumulative changes in heading between all successive pairs of locations in the object's state
  def getCumulativeHeading: Double = {
    var diff: Double = 0.0D
    val iter = objHistory.reverseIterator
    var first = iter.next()
    while (iter.hasNext)
    {
        var second = iter.next()
        diff += MobilityChecker.slopeDifference(first.getHeading, second.getHeading)
        first = second
    }
    diff         //Return angle value between -180 and 180 degrees
  }

}