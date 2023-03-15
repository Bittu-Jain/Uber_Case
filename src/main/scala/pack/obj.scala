package pack

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import scala.io.Source

object obj {


	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Uber_Case").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder.getOrCreate()
					import spark.implicits._

					val df = spark.read.format("csv").option("header", "true")
					.load("file:///c://data/anblicks.txt")

					df.show()

					df.createOrReplaceTempView("temp")

					val df1 = spark.sql("""select product,
					  floor(total_quantity/monthly_sale)%30 as month,
					  ((total_quantity%monthly_sale)) as days
					    from temp""")

					df1.show()



	}

}