import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._




object Task3 extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Introduction to DataSet")
    .config("spark.master", "local")
    .getOrCreate()

  case class TaxiZoneFact(
                           VendorID: Int,
                           tpep_pickup_datetime: String,
                           tpep_dropoff_datetime: String,
                           passenger_count: Int,
                           trip_distance: Double,
                           RatecodeID: Int,
                           store_and_fwd_flag: String,
                           PULocationID: Int,
                           DOLocationID: Int,
                           payment_type: Int,
                           fare_amount: Double,
                           extra: Double,
                           mta_tax: Double,
                           tip_amount: Double,
                           tolls_amount: Double,
                           improvement_surcharge: Double,
                           total_amount: Double
                         )

  val taxiDistrictDF = spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/taxi_zones.csv")

  val taxiFactTableDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  //taxiDistrictDF.show(7)

  //taxiFactTableDF.show(7)

  //taxiFactTableDF.printSchema()

  val taskresult = taxiDistrictDF
    .join(taxiFactTableDF, col("LocationID") === col("PULocationID"))
    .groupBy(col("Borough"), col("Zone"))
    .agg(
      count("*").as("total_trips")
      , round(sum(col("total_amount")), 2).as("total_amount")
      , round(avg(col("total_amount")), 2).as("avg_amount")
      , round(avg(col("trip_distance")), 2).as("avg_trip_distance")
      , min(col("trip_distance")).as("min_trip_distance")
      , max(col("trip_distance")).as("max_trip_distance")
    )
    .orderBy(col("Borough"), col("Zone"))


  taskresult.show(10)

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val tbl = "public.taxi_task3"
  val user = "postgre"
  val password = "postgre"
  val tblcolumns = "Borough VARCHAR(64), Zone VARCHAR(64), total_trips INT, total_amount REAL, avg_amount REAL, avg_trip_distance REAL,  min_trip_distance REAL, max_trip_distance REAL"

  taskresult.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tbl)
    .option("user", user)
    .option("password", password)
    .option("createTableColumnTypes", tblcolumns)
    .save()

}
