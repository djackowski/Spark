import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object main extends App {
  System.setProperty("hadoop.home.dir", "E:\\Spark")
  val conf = new SparkConf().set("spark.sql.warehouse.dir", "file:///E:\\Spark")
  val spark = SparkSession
    .builder
    .config(conf)
    .master("local[*]")
    .getOrCreate

  val df = spark.read.csv("E:\\Spark\\tracks.csv")

  val col = df.rdd

  col.flatMap(record => if (record(2).toString == "Queen") Some(record(3)) else None).foreach(println)

  val count = col.flatMap(record => if (record(2).toString == "Queen") Some(record(3)) else None).count
  println(count)
}