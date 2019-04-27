import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import loaders.ReadAndWrite

class ReadAndWriteTestSpec extends FunSuite with BeforeAndAfterEach{

  private val master = "local"

  private val appName = "ReadAndWrite-Test"

  var spark : SparkSession = _

  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  test("creating data frame from parquet file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = spark.read.json("src/test/resources/people.json")
    peopleDF.write.mode(SaveMode.Overwrite).parquet("src/test/resources/people.parquet")

    val df = ReadAndWrite.readParquetFile(sparkSession,"src/test/resources/people.parquet")
    df.printSchema()

  }

 test("Reading files of different format using readParquetFile should throw an exception") {
   intercept[Exception] {
     val sparkSession = spark

     val df = ReadAndWrite.readParquetFile(sparkSession,"src/test/resources/people.txt")
     df.printSchema()

   }
 }

  test("creating data frame from text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.printSchema()
  }

  test("counts should match with number of records in a text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.printSchema()

    assert(peopleDF.count() == 3)
  }

  test("data should match with sample records in a text file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.printSchema()

    assert(peopleDF.take(1)(0)(0).equals("Michael"))
  }

  test("Write a data frame as csv file") {
    val sparkSession = spark
    import sparkSession.implicits._
    val peopleDF = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    //header argument should be boolean to the user to avoid confusions
    ReadAndWrite.writeDataframeAsCSV(peopleDF,"src/test/resources/out.csv",java.time.Instant.now().toString,",","true")
  }


  test("Reading files of different format using readTextfileToDataSet should throw an exception") {
    intercept[org.apache.spark.sql.AnalysisException] {
    val sparkSession = spark

    import org.apache.spark.sql.functions.col

    val df = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/people.parquet")
    df.select(col("name"))

     }
  }

  test("Reading an invalid file location using readTextfileToDataSet should throw an exception") {
    intercept[Exception] {
      val sparkSession = spark

      import org.apache.spark.sql.functions.col

      val df = ReadAndWrite.readTextfileToDataSet(sparkSession,"src/test/resources/invalid.txt")
      df.show()

    }
  }

  test("Reading an invalid file location using readParquetFile should throw an exception") {
    intercept[Exception] {
      val sparkSession = spark

      val df = ReadAndWrite.readParquetFile(sparkSession,"src/test/resources/invalid.parquet")
      df.show()

    }
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

}

case class Person(name: String, age: Int)
