package org.edu
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object agg {
def main: Unit = {


  import sys.process._
  "rm -rf /tmp/chk_eroshkin".!!

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("eroshkin_lab04a")
      .getOrCreate()
  import spark.implicits._

  def killAll() = {
    SparkSession
      .active
      .streams
      .active
      .foreach { x =>
        val desc = x.lastProgress.sources.head.description
        x.stop
        println(s"Stopped ${desc}")
      }
  }

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "andrey_eroshkin"
  )

  val sdf = spark
    .readStream
    .format("kafka")
    .options(kafkaParams)
    .load

  val parsedSdf = sdf.select('value.cast("string"), 'topic, 'partition, 'offset)

  killAll

  val schema = StructType(Seq(
    StructField("category", StringType, true),
    StructField("event_type", StringType, true),
    StructField("item_id", StringType, true),
    StructField("item_price", LongType, true),
    StructField("timestamp", LongType, true),
    StructField("uid", StringType, true)
  ))

  val df = parsedSdf
    .withColumn("jsonData", from_json(col("value"), schema))
    .select("jsonData.*")
    .withColumn("date", ($"timestamp" / 1000).cast(TimestampType))

  val dfAgg = df
    .groupBy(window(col("date"), "1 hours"/*, "5 seconds"*/))
    .agg(
      sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).alias("revenue"),
      sum(when(col("uid").isNotNull, 1).otherwise(0)).alias("visitors"),
      sum(when(col("event_type") === "buy", 1).otherwise(0)).alias("purchases")
    )

  val dfRes = dfAgg.withColumn("aov", col("revenue")/col("purchases"))
    .withColumn("start_ts", col("window.start").cast("long"))
    .withColumn("end_ts", col("window.end").cast("long"))
    .drop(col("window"))

  dfRes
    .writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    //     .option("checkpointLocation", s"/tmp/chk/$chkName")
    .option("truncate", "false")
    .option("numRows", "20")
    .outputMode("update")
    .start

  val query = dfRes
    .selectExpr("CAST(start_ts AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .format("kafka")
    .option("checkpointLocation", "/tmp/chk_eroshkin")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("topic", "andrey_eroshkin_lab04b_out")
//    .option("maxOffsetsPerTrigger", 200)
    .outputMode("update")
    .start()


  query.awaitTermination()



}
}
