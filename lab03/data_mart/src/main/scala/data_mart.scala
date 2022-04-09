package org.edu

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions


class data_mart {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("eroshkin_lab03")
      .getOrCreate()

  // TODO Вынести в файл
  val user = "andrey_eroshkin"
  val password = ""

  def main(args: Array[String]): Unit = {

    spark.setCassandraConf(
      CassandraConnectorConf.KeepAliveMillisParam.option(1000) ++
        CassandraConnectorConf.ConnectionHostParam.option("10.0.0.31") ++
        CassandraConnectorConf.ReadTimeoutParam.option(240000) ++
        CassandraConnectorConf.ConnectionPortParam.option("9042"))

    val dfClients = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "labdata", "table" -> "clients"))
      .load()


    // age_cat – категория возраста, одна из пяти: 18-24, 25-34, 35-44, 45-54, >=55.
    val dfClients1 = dfClients
      .filter(col("uid").isNotNull)
      .withColumn("age_cat",
        when(col("age") < 18, "<18")
          .when(col("age") <= 24, "18-24")
          .when(col("age") <= 34, "25-34")
          .when(col("age") <= 44, "35-44")
          .when(col("age") <= 54, "45-54")
          .otherwise(">=55"))
      .select("uid", "age_cat", "gender")

    val esOptions =
      Map(
        "es.nodes" -> "10.0.0.31:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"
      )

    val dfLogs = spark
      .read
      .format("es")
      .options(esOptions)
      .load("visits*")


    val dfLogs1 = dfLogs
      .filter(col("uid").isNotNull)
      //     .filter(col("event_type") === "view") // В задаче было число посещений считать, но покупка- тоже посещение
      .withColumn("category_lowercase", lower(col("category")))
      .withColumn("category_clean", regexp_replace(col("category_lowercase"), "[ -]", "_"))
      .withColumn("category_full", concat(lit("shop_"), col("category_clean")))
      .drop("category")
      .withColumnRenamed("category_full", "category")
      .select("uid", "category")

    val shopPivot = dfLogs1
      .groupBy("uid")
      .pivot("category")
      .count

    val dfLogsWeb = spark.read.json("hdfs:///labs/laba03/weblogs.json")
      .withColumn("vis", explode(col("visits")))
      .select("uid", "vis")

    val dfLogsWeb1 = dfLogsWeb
      .select(col("uid"), col("vis.*"))
      .withColumn("url1", regexp_replace(col("url"), "^https*://https*://", "http://"))
      .withColumn("suffix", split(col("url1"), "//")(1))
      .withColumn("domain", regexp_replace(split(col("suffix"), "/")(0), "^www\\.", ""))
      .select("uid", "domain")


    val dfWebCat = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()

    val dfWebCat1 = dfWebCat
      .withColumn("category_lowcase", lower(col("category")))
      .withColumn("category_clean", regexp_replace(col("category_lowcase"), "[ -]", "_"))
      .withColumn("category_full", concat(lit("web_"), col("category_clean")))
      .drop("category")
      .withColumnRenamed("category_full", "category")
      .select("domain", "category")


    val web = dfLogsWeb1.join(dfWebCat1, Seq("domain"), "inner")


    val webPivot = web
      .groupBy("uid")
      .pivot("category")
      .count


    val res = dfClients1
      .join(webPivot, Seq("uid"), "left")
      .join(shopPivot, Seq("uid"), "left")

    res.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/andrey_eroshkin")
      .option("dbtable", "clients")
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()

    spark.stop

  }
}
