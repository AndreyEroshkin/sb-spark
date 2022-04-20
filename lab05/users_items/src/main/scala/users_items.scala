import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class readInput(path: String, prefix: String) {
  val spark: SparkSession = SparkSession.getActiveSession.getOrElse {
    throw new IllegalArgumentException("Could not find active SparkSession")
  }
  val df: DataFrame = spark
    .read
    .format("json")
    .load(path)
    .persist()

  val maxDate: Int = df
    .select(from_unixtime(col("timestamp") / 1000, "yyyyMMdd"))
    .collect()(0)(0)
    .toString.toInt

  val prepared: DataFrame = df
    .filter(col("uid").isNotNull)
    .withColumn("item",
      lower(concat(lit(prefix),
        regexp_replace(col("item_id"), "[ -]", "_"))))
    .select("uid", "item")

  val matrix: DataFrame = prepared
    .groupBy("uid")
    .pivot("item")
    .count

}


object users_items {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("eroshkin_lab05")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()


    val update: String = spark.conf.get("spark.users_items.update") // : 0 или 1 режим работы, сделать новую матрицу users_items или добавить строки к существующей. По умолчанию - 1, добавить строки в матрицу.
    val outputDir: String = spark.conf.get("spark.users_items.output_dir") // : абсолютный или относительный путь к выходным данным.
    val inputDir: String = spark.conf.get("spark.users_items.input_dir") // : абсолютный или относительный путь к входным данным.

    if (update == "0") {
      initialLoad(inputDir: String, outputDir: String)
    }
    else if (update == "1") {
      updateLoad(inputDir: String, outputDir: String)
    }
    spark.stop
  }

  def initialLoad(inputDir: String, outputDir: String): Unit = {
    val view = new readInput(path = inputDir + "/view/*", prefix = "view_")
    val buy = new readInput(path = inputDir + "/buy/*", prefix = "view_")
    val result = view.matrix.join(buy.matrix, Seq("uid"), "full")
    val maxDate = view.maxDate.max(buy.maxDate)

    result.write
      .format("parquet")
      .mode("overwrite")
      .save(outputDir + s"/$maxDate")
  }

  def unionDiffDF(df1: DataFrame, df2: DataFrame): DataFrame = {
    //    val merged_cols = df1.columns.toSet ++ df2.columns.toSet
    //    import org.apache.spark.sql.functions.{col, lit}
    //    def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
    //      merged_cols.toList.map(x => x match {
    //        case x if column.contains(x) => col(x)
    //        case _ => lit(null).as(x)
    //      })
//    val new_df1 = df1.select(getNewColumns(df1.columns.toSet, merged_cols): _*)
//    val new_df2 = df2.select(getNewColumns(df2.columns.toSet, merged_cols): _*)
//
//    val merged_df = new_df1.unionByName(new_df2)
//
//    merged_df
    def union_cols(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x
      match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(0).as(x)
      }
      )
    }

    val df1Cols = df1.columns.toSet
    val df2Cols = df2.columns.toSet
    val total = df1Cols ++ df2Cols

    val resMatrix = df1.select(union_cols(df1Cols, total): _*).union(df2.select(union_cols(df2Cols, total): _*))

    resMatrix
  }


def readOutput(path: String): DataFrame = {
  val spark = SparkSession.getActiveSession.getOrElse {
    throw new IllegalArgumentException("Could not find active SparkSession")
  }

  val df = spark
    .read
    .format("parquet")
    .load(path + "/20200429/*")
    .persist()
  df.count
  df
}

def updateLoad(inputDir: String, outputDir: String): Unit = {

  val view = new readInput(path = inputDir + "/view/*", prefix = "view_")
  val buy = new readInput(path = inputDir + "/buy/*", prefix = "buy_")
//  val newData = view.prepared
//    .union(buy.prepared)
//    .groupBy("uid")
//    .pivot("item")
//    .count
  val newData = view.matrix.join(buy.matrix, Seq("uid"), "full")
  val maxDate = view.maxDate.max(buy.maxDate)

  val oldData = readOutput(outputDir)

  val resMatrix = unionDiffDF(oldData, newData)

  resMatrix
    .write
    .format("parquet")
    .mode("overwrite")
    .save(outputDir + s"/$maxDate")
}


}
