package org.edu

/*
Kafka bootstrap: spark-master-1:6667

Топик Kafka: lab04_input_data

Схема для сохранения на hdfs: /user/name.surname/visits/view/ и /user/name.surname/visits/buy/

I. Задача с высоты птичьего полета
Вам нужно:

считать события из топика Kafka lab04_input_data, используя read.format("kafka"). Данные в этом топике уже есть. Описание данных ниже.

записывать события с простыми посещениями страниц в HDFS по пути /user/name.surname/visits/view/$date, где $date - дата в формате YYYYMMDD, например 20200501.

записывать события с простыми посещениями страниц в HDFS по пути /user/name.surname/visits/view/$dateColumn=$date, где $date - дата в формате YYYYMMDD, например p_date=20200501. Партиционировать нужно по полю, отличному от поля date. Иначе поле date не попадет в сам файл.
а события с покупками – в путь /user/name.surname/visits/buy/$date.
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.process._


class filter {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("eroshkin_lab03")
      .getOrCreate()

  def main(arg: Array[String]): Unit = {

    object filter {

      def main(args: Array[String]): Unit = {
        import spark.implicits._

        val dir = spark.sparkContext.getConf.getOption("spark.filter.output_dir_prefix") // путь (полный или относительный), куда будут писаться фильтрованные данные.
        val topicName = spark.sparkContext.getConf.getOption("spark.filter.topic_name") //название топика для чтения
        val offset = spark.sparkContext.getConf.getOption("spark.filter.offset") // оффсет в нулевой партиции топика, с которого должно происходить чтение. Также принимаются значение "earliest".

//        if (offset != "earliest") {
//          offset = s"""{"$topic":{"0":$offset}}"""
//        }
//
//        val kafkaParams = Map(
//          "kafka.bootstrap.servers" -> "spark-master-1:6667",
//          "subscribe" -> topic,
//          "startingOffsets" -> offset
//        )

        var kafkaParams = Map(
          "kafka.bootstrap.servers" -> "spark-master-1:6667"
        )

        if(topicName.isDefined) {
          kafkaParams += ("subscribe" -> topicName.get)
        }

        if((offset.isDefined) & (topicName.isDefined)){
          kafkaParams += ("startingOffsets" -> s"""{"$topicName.get":{"0":$offset.get}}""")
        }


        val df = spark
          .read
          .format("kafka")
          .options(kafkaParams)
          .load

        val jsonString = df
          .select(col("value").cast("string"))
          .as[String]

        val parsed = spark
          .read
          .json(jsonString)
          .withColumn("date", from_unixtime(col("timestamp") / 1000, "yyyyMMdd"))
          .withColumn("p_date", col("date"))

        val views = parsed
          .filter(col("event_type") === "view")
        val buy = parsed
          .filter(col("event_type") === "buy")

        views
          .write
          .format("json")
          .mode("overwrite")
          .partitionBy("p_date")
          .save(dir + "/view")

        buy
          .write
          .format("json")
          .mode("overwrite")
          .partitionBy("p_date")
          .save(dir + "/buy")
      }
    }
  }
}