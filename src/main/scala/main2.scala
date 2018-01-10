import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object main2 extends App {
  System.setProperty("hadoop.home.dir", "E:\\Spark")
  val conf = new SparkConf().set("spark.sql.warehouse.dir", "file:///E:\\Spark")
  val spark = SparkSession
    .builder
    .config(conf)
    .master("local[*]")
    .getOrCreate

  val df = spark.read.csv("E:\\Spark\\tracks.csv")

  val col = df.rdd

  val counts = col.flatMap(record => Some(record(2)))
    .map(x => (x, 1))
    .reduceByKey((a, b) => a + b).collect()

  counts.foreach({ case (x, y) => if (y > 150) println(x, y) })


}