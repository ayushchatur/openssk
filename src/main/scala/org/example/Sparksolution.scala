package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, lower, row_number, sum, trim}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.Console.println
import scala.math.Fractional.Implicits.infixFractionalOps
//import org.apache.spark.sql.impl

//import org.apache.spark.implicits._
object Sparksolution {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    //Create RDD from external Data source
    import spark.implicits
    val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true"))
      .csv("/home/ayushchatur/sales_data_sample.csv")
    val rdd = df.rdd
    case class Shipped(order_num: Int , yearid: Int, productline: String, price: Double, status: String)
val shippedrddmap = rdd.map (_.toString() )
//    shippedrddmap.foreach( f => { println(f + " endl" + f.getClass())})

//
    val shipped = shippedrddmap.map(
    line => {
      val fields = line.substring(1, line.length() - 1).split(",")
//      println(fields  + "class" + fields.getClass())
    (fields(0).toInt, fields(9).toInt, fields(10),fields(2).toDouble, fields(6))

    })
//    }
//  shipped = shipped.foreach( f => { println(f)})
    println(shipped.count())

    val filtered = shipped.filter( _._5.toString().toLowerCase() == "shipped")


    val new_key = filtered.map(
      k => (k._2.toString()+k._3.toString(), k )
    )

    // rdd (price, 1)
    def createCombiner = (price:Double) =>
      (price, 1)

    val mergeval = (acc: (Double, Int), element: (Double)) =>
      (acc._1 + element, acc._2 + 1)

    val mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    val another_new_key = new_key.map { x => (x._1, x._2._4) }

    val rdd2 = another_new_key.combineByKey(
      createCombiner,
      mergeval,
      mergeCombiner
    )



    val final_rdd = rdd2.map(x => (x._1, ((x._2._1 / x._2._2 * 100).round / 100.toDouble)))
//    val final_rdd = rdd2.map(x => (x._1, (x._2 % 0.01)))
    println("~~~~~~~~~~~~~~~~~~~")
    final_rdd.foreach(f => {
      println(f)
    })

    val split_rdd = final_rdd.map( x => ( ((x._1.substring(0,4),x._1.substring(4,x._1.length )),  x._2))
    )
    println("################")
    split_rdd.foreach(f => {
      println(f)
    })
    val sorted = split_rdd.sortByKey()
    println("__________________________")
    sorted.foreach(f => {
      println(f)
    })
    println("%%%%%%%%%%%%%%%%")
    val rowRDD:RDD[Row] = sorted.map(p => Row(p._1._1, p._1._2, p._2))
    case class Shipp( yearid: String, productline: String, price: Double)
//    val hth = sorted.map( p => Shipp(p._1._1, p._1._2, p._2))
val schema = new StructType()
  .add(StructField("YEAR_ID", StringType, false))
  .add(StructField("PRODUCTLINE", StringType, false))
  .add(StructField("AVERAGE_SALES_AMT", DoubleType, false))
    val dd = spark.createDataFrame(rowRDD, schema)
    dd.coalesce(1).write.option("header", true).csv("result_cs")
//      .toDf().write.csv("")


//    val dff = spark.createDataFrame(finalt, Result.getClass)



//    finalt.saveAsTextFile("result.txt")
//    finalt.toDf()
    //    ) // sum per uniq key


  }
}
