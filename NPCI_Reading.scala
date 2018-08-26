import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object myConsumer2 {

  def main(args: Array[String]) {

    val rootLogger = Logger.getRootLogger() //deactivate spark log msgs
    rootLogger.setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .appName("NPCIChallenge")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/home/humberto/Downloads/Demo_output.csv")

    df.show()

    df.createOrReplaceTempView("Customers")

    df.sqlContext
      .sql("SELECT sum(amount) AS Expenses, sourceId " +
        "FROM Customers " +
        "GROUP BY sourceId order by 1 Desc").show

    df.sqlContext
      .sql("SELECT sum(amount) AS Expenses, sourceId " +
        "FROM Customers " +
        "GROUP BY sourceId order by 1 Desc Limit 1").show

    //val distinctYears = sqlContext.sql("select distinct Year from names")

    /*
    val lines2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "NPCIINPUT")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("cast (value as string) as json")

      val query2 = lines2.writeStream
      .outputMode("append")
      .format("console")
      .start()

    //lines2.printSchema()
    query2.awaitTermination()
*/
  }

}
