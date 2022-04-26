import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


class train {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("eroshkin_lab07_train")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()

    /*
    * Программа train.scala должна принимать следующие аргументы через spark.conf:
    * путь к тренировочному датасету (в HDFS)
    * путь для сохранения spark-pipeline (в HDFS)
    * */

    val USER: String = "andrey.eroshkin"
    val JSON_DIR = s"/labs/laba07/laba07.json"
    val MODEL_PATH = s"lab07_model"

    val visit = spark.read
      .json(JSON_DIR)
      .withColumn("visits", explode(col("visits")))
      .select(col("*"), col("visits.*"))
      .drop("visits")
      .na.drop(List("uid"))
      .withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .toDF
      .na.drop(List("url"))
      .persist

    val train_df = visit
      .groupBy("uid", "gender_age")
      .agg(collect_list(col("url")).as("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val labels = indexer.fit(train_df).labels

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val revIndexer = new IndexToString()
      .setInputCol("prediction")
      .setLabels(labels)
      .setOutputCol("res")

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, revIndexer))

    val model = pipeline.fit(train_df)

    model
      .write
      .overwrite()
      .save(MODEL_PATH)
  }
}
