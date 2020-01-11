/******************************************************************************
  * Project: datAcron (http://ai-group.ds.unipi.gr/datacron/)
  * Task: 2.1 Trajectory detection & summarization
  * Module: Synopses Generator
  * File: eu.datacron.synopses.aviation/CriticalPointSchema.scala
  * Description: Apache Kafka serialization of critical points for the AVIATION use case. Attribute schema must conform to the corresponding AVRO specification (file: critical_point.avsc).
  * Developer: Kostas Patroumpas (UPRC)
  * Created: 15/12/2016
  * Revised: 6/6/2017
  ************************************************************************/

package eu.datacron.synopses.aviation

import scala.util.parsing.json._

import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import scala.collection.immutable.Map

  object CriticalPointSchema extends SerializationSchema[critical_point, Array[Byte]] with DeserializationSchema[critical_point] {

    override def serialize(t: critical_point): Array[Byte] = t.toString().getBytes("UTF-8")

    override def isEndOfStream(t: critical_point): Boolean = false

    override def deserialize(bytes: Array[Byte]): critical_point = {
      var jsonString = new String(bytes, "UTF-8")
      val data = JSON.parseFull(jsonString)
      val globalMap = data.get.asInstanceOf[Map[String, critical_point]]
      new critical_point(globalMap.get("timestamp").get.asInstanceOf[Double].toLong, globalMap.get("id").get.asInstanceOf[CharSequence], globalMap.get("longitude").get.asInstanceOf[Double], globalMap.get("latitude").get.asInstanceOf[Double], globalMap.get("altitude").get.asInstanceOf[Double], globalMap.get("annotation").get.asInstanceOf[critical_point_annotation], globalMap.get("distance").get.asInstanceOf[Double], globalMap.get("rate_of_climb").get.asInstanceOf[Double], globalMap.get("speed").get.asInstanceOf[Double], globalMap.get("heading").get.asInstanceOf[Double], globalMap.get("time_elapsed").get.asInstanceOf[Double].toLong, globalMap.get("on_ground").get.asInstanceOf[String], globalMap.get("ingestion_time").get.asInstanceOf[Double].toLong)
    }

    override def getProducedType: TypeInformation[critical_point] = TypeExtractor.getForClass(classOf[critical_point])
  }
