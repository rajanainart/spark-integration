package com.rajanainart

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.util.parsing.json.JSON

class JsonConfig (resource : String, dataType : Int) {
  var json : String = ""

  if (dataType == 1) {
    var stream  = Main.getClass.getResourceAsStream(resource)
    if (stream == null)
      stream    = Main.getClass.getResourceAsStream("/"+resource)

    json = scala.io.Source.fromInputStream(stream)
                      .getLines
                      .toList
                      .mkString(" ")
                      .trim
                      .replaceAll("\\s+", " ")
    stream.close()
  }
  if (dataType == 2)
    json = resource

  val jsonObject = JSON.parseFull(json)

  def getJsonAsString    () : String              = json
  def getJsonAsRDD       () : RDD[String]         = Common.sparkSession.sparkContext.parallelize(List(getJsonAsString()))
  def getJsonAsDataSet   () : Dataset[Row]        = Common.sparkSession.read.json(getJsonAsRDD())
  def getJsonAsProperties() : Map[String, Object] = jsonObject.get.asInstanceOf[Map[String, Object]]

  def getJsonPropertiesStartsWith(startsWith : String) : mutable.Map[String, Object] = {
    val properties = getJsonAsProperties()
    val result : mutable.Map[String, Object] = new mutable.HashMap[String, Object]
    for (key <- properties.keySet) {
      if (key.startsWith(startsWith))
        result.put(key, properties.get(key).get)
    }
    result
  }

  def getJsonProperty(property : String) : Object = {
    val properties = getJsonAsProperties()
    if (properties.contains(property))
      return properties.get(property).get
    null
  }
}
