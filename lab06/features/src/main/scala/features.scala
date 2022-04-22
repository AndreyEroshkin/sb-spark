import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.URL
import java.net.URLDecoder.decode
import scala.util.Try

object features {
  def main(args: Array[String]): Unit = {


    val USER: String = "andrey.eroshkin"
    val HDFS_DIR = s"/user/$USER/users-items/20200429"
    val JSON_DIR = s"/labs/laba03/weblogs.json"
    val OUT_DIR = s"/user/$USER/features"

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("eroshkin_lab05")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()

    val decode_url = udf { (url: String) => Try(new URL(decode(url, "UTF-8")).getHost).toOption }

    val visit = spark
      .read
      .json(JSON_DIR)
      .select(col("uid"), explode(col("visits")))
      .select(col("uid"), col("col.*"))
      .toDF

    val webLogs = visit
      .filter(col("uid").isNotNull)
      .withColumn("timestamp", to_utc_timestamp(from_unixtime(col("timestamp") / 1000), "UTC"))
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "^www\\.", ""))

    val top1000 = webLogs
      .groupBy(col("domain"))
      .count
      .na.drop(List("domain"))
      .orderBy(col("count").desc)
      .limit(1000)
      .orderBy(col("domain").asc)
      .select("domain")
      .rdd.map(r => r(0)).collect

    val topWebLogs = webLogs
      .filter(col("domain").isInCollection(top1000))
      .select("uid", "domain")

    topWebLogs.show

    val matrix = topWebLogs
      .groupBy("uid")
      .pivot("domain")
      .count
      .na.fill(0)

    val col_arr = matrix.columns.filter(_ != "uid")

    val topDomainFeatures = matrix
      .select(col("uid"), array(col_arr.map(x => col(s"`${x}`")): _*).alias("domain_features"))

    topDomainFeatures.show

    val weekDayMatrix = webLogs
      .withColumn("day_of_week", concat(lit("web_day_"), lower(date_format(col("timestamp"), "E"))))
      .drop("timestamp")
      .groupBy("uid", "day_of_week")
      .count
      .groupBy("uid")
      .pivot("day_of_week")
      .sum("count")

    val hoursMatrix = webLogs
      .withColumn("hour", concat(lit("web_hour_"), date_format(col("timestamp"), "k")))
      .drop("timestamp")
      .groupBy("uid", "hour")
      .count
      .groupBy("uid")
      .pivot("hour")
      .sum("count")

    val fractWebHours = webLogs
      .withColumn("hour", date_format(col("timestamp"), "k"))
      .drop("timestamp")
      .groupBy("uid")
      .agg(
        (sum(when(col("hour") >= 9 && col("hour") < 18, 1)
          .otherwise(0)) / sum(when(col("hour") >= 0 && col("hour") <= 23, 1)
          .otherwise(0)))
          .alias("web_fraction_work_hours"),
        (sum(when(col("hour") >= 18 && col("hour") <= 23, 1)
          .otherwise(0)) / sum(when(col("hour") >= 0 && col("hour") <= 23, 1)
          .otherwise(0)))
          .alias("web_fraction_evening_hours")
      )

    val usersItems = spark.read.parquet(HDFS_DIR)

    val webDF = topDomainFeatures
      .join(weekDayMatrix, Seq("uid"), "inner")
      .join(hoursMatrix, Seq("uid"), "inner")
      .join(fractWebHours, Seq("uid"), "inner")

    val result = usersItems.join(webDF, Seq("uid"), "full")

    result
      .write
      .format("parquet")
      .mode("overwrite")
      .save(OUT_DIR)

  }
}
