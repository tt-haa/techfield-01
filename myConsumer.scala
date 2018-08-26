
import org.apache.log4j.{Level, Logger}

import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions._
import java.io.FileWriter
import java.io.BufferedWriter

import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

//import the Cassandra connector
import com.datastax.spark.connector._

import com.datastax.spark.connector.streaming._

object myConsumer {

  def main(args: Array[String]) {

    val ssc  = new StreamingContext("local[*]","streamNPCI",Seconds(3))

    val rootLogger = Logger.getRootLogger() //deactivate spark log msgs
    rootLogger.setLevel(Level.ERROR)

    val topics = List("NPCIINPUT").toSet

    val kafkaParams = Map(

      "bootstrap.servers"-> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "earliest")

    val line = KafkaUtils.createDirectStream[String,String](

      ssc, PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    line.map(record=>(record.value().toString)).print

    val trax = line.map(record=>
      (record.value()
      .replace("b0dd3f13-fbca-","")
      .replace("-49790bc3bb04~#~1~#~",",")
      .replace("~#~3~#~4011~#~5~#~",",")
      .replace("~#~9~#~",",")
      .replace("~#~366~#~12~#~13~#~14~#~15~#~16~#~",",")
      .replace("~#~00~#~SUCCESS","")
      .replace("~#~",",")
      ))

    trax.print

    val trax2 = List(trax).toArray

    //val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.implicits._


    //trax.saveAsTextFiles("/home/humberto/Downloads/txtOutput.txt")

    ///******************

    val stringAry: Array[String]  = (List(trax2.toList) map (_.toString)).toArray

    val out = new BufferedWriter(new FileWriter("/home/humberto/Downloads/employee.csv"));
    val writer = new CSVWriter(out);

    val employeeSchema=Array("name","age","dept")

    val employee1= Array("piyush","23","computerscience")

    val employee2= Array("neel","24","computerscience")

    val employee3= Array("aayush","27","computerscience")

    var listOfRecords= List(employeeSchema,stringAry)

    writer.writeAll(listOfRecords)

    out.close()
    ssc.start()
    ssc.awaitTermination
    //mappedData.saveToCassandra("tutorial","cassandratable3")
  }

}
