import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD



object Task2 extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val contextrdd: SparkContext = spark.sparkContext


  case class TaxiZoneFact(
                           VendorID: String,
                           tpep_pickup_datetime: String,
                           tpep_dropoff_datetime: String,
                           passenger_count: String,
                           trip_distance: String,
                           RatecodeID: String,
                           store_and_fwd_flag: String,
                           PULocationID: String,
                           DOLocationID: String,
                           payment_type: String,
                           fare_amount: String,
                           extra: String,
                           mta_tax: String,
                           tip_amount: String,
                           tolls_amount: String,
                           improvement_surcharge: String,
                           total_amount: String
                         )

  val TaxiFactRDD: RDD[Row] = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018").rdd

  val taskresult = TaxiFactRDD
    .map(t => TaxiZoneFact(t(0).toString, t(1).toString, t(2).toString, t(3).toString, t(4).toString, t(5).toString, t(6).toString, t(7).toString, t(8).toString, t(9).toString, t(10).toString, t(11).toString, t(12).toString, t(13).toString, t(14).toString, t(15).toString, t(16).toString))
    .map(tp => (tp.tpep_pickup_datetime.substring(11, 13), 1))
    .reduceByKey(_ + _)
    .sortBy(x => x._2, false)

  val value1 = taskresult.foreach(x => println(x))

  val value2 = taskresult.saveAsTextFile("src/main/resources/data/result_data/task2result")

}
