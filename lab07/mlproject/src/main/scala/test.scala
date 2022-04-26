import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

class test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("eroshkin_lab07_test")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()

    /*
    * Программа train.scala должна принимать следующие аргументы через spark.conf:
    * путь к тренировочному датасету (в HDFS)
    * путь для сохранения spark-pipeline (в HDFS)
    */

    val USER: String = "andrey.eroshkin"
    val JSON_DIR = s"/labs/laba07/laba07.json"
    val MODEL_PATH = s"lab07_model"

    val dfInput = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "lab07_in")
      .load()
      .selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("uid", StringType)
      .add("visits", ArrayType(new StructType()
        .add("url", StringType)
        .add("timestamp", LongType)))

    val dfUnpacked = dfInput
      .withColumn("jsonData", from_json(col("value"), schema))
      .select("jsonData.uid", "jsonData.visits")
      .withColumn("url", explode(col("visits.url")))
      .withColumn("domains", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("domains"), "www.", ""))
      .select("uid", "domains")
      .groupBy("uid").agg(collect_list("domains").as("domains"))

    val model = PipelineModel.load("lab07_model")

//    val converter = new IndexToString().setInputCol("prediction").setOutputCol("label_string").setLabels(indexer.labels)

    val dfPredict = model.transform(dfUnpacked)
      .withColumnRenamed("res", "gender_age")
      .select(col("uid"), col("gender_age"))

    val query = dfPredict
      .selectExpr("CAST(uid AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .option("checkpointLocation", "lab07-chk")
      .option("kafka.bootstrap.servers", "10.0.0.5:6667")
      .option("topic", "andrey_eroshkin_lab07_out")
      .option("maxOffsetsPerTrigger", 200)
      .outputMode("update")
      .start

    query.awaitTermination
  }
}
