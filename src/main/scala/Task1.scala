import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame



object Task1 extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiDistrictDF = spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/taxi_zones.csv")

  val taxiFactTableDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  /*
  val taxiMostPopularDistrictsDF = spark.sql("""
                                   |select
                                   |  tz.Borough,
                                   |  count(*) as `total trips`
                                   |from taxiFactTableDF tf
                                   |  left join taxiDistrictDF tz
                                   |  on tz.LocationID = tf.PULocationID
                                   |group by tz.Borough
                                   |order by `total trips` desc
    """.stripMargin)

   */

  //taxiMostPopularDistrictsDF.show()

    val taskresult = taxiFactTableDF
    .join(taxiDistrictDF, col("PULocationID") === col("LocationID"))
    .groupBy("Borough")
    .agg(
      count("*").as("total trips")
    )
    .orderBy(col("total trips").desc)

  taskresult.write.mode("overwrite").parquet("src/main/resources/data/result_data/task1result/")

  taskresult.show()


}
