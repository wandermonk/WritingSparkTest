package loaders

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ReadAndWrite {

  // function to get parquet dataset from s3
  def readParquetFile(spark: SparkSession,
                      locationPath: String): DataFrame = {
    spark.read
      .parquet(locationPath)
  }

  // function to get text dataset from s3
  def readTextfileToDataSet(spark: SparkSession,
                            locationPath: String): Dataset[String] = {
    spark.read
      .textFile(locationPath)
  }

  // function to write RDD as Text File in s3
  def writeDataframeAsCSV(df: DataFrame,
                          locationPath: String,
                          oDate: String,
                          delimiter: String,
                          header: String
                         ): Unit = {
    df.write
      .option("delimiter", delimiter)
      .option("header", header)
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(locationPath + "/" + oDate + "/")
  }

}