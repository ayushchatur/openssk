package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower, trim}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructType}

object SparkSessionTest {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();
    
    println("First SparkContext:")
    println("APP Name :"+spark.sparkContext.appName);
    println("Deploy Mode :"+spark.sparkContext.deployMode);
    println("Master :"+spark.sparkContext.master);

//    val sparkSession2 = SparkSession.builder()
//      .master("local[1]")
//      .appName("SparkByExample-test")
//      .getOrCreate();
//
//    println("Second SparkContext:")
//    println("APP Name :"+sparkSession2.sparkContext.appName);
//    println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
//    println("Master :"+sparkSession2.sparkContext.master);
//    val schema = new StructType()
//      .add("ORDERNUMBER", IntegerType, true)
//      .add("QUANTITYORDERED", IntegerType, true)
//      .add("PRICEEACH", DoubleType, true)
//      .add("ORDERLINENUMBER", StringType, true)
//      .add("State", StringType, true)
//      .add("LocationType", StringType, true)
//      .add("Lat", DoubleType, true)
//      .add("Long", DoubleType, true)
//      .add("Xaxis", IntegerType, true)
//      .add("Yaxis", DoubleType, true)
//      .add("Zaxis", DoubleType, true)
//      .add("WorldRegion", StringType, true)
//      .add("Country", StringType, true)
//      .add("LocationText", StringType, true)
//      .add("Location", StringType, true)
//      .add("Decommisioned", BooleanType, true)
//      .add("TaxReturnsFiled", StringType, true)
//      .add("EstimatedPopulation", IntegerType, true)
//      .add("TotalWages", IntegerType, true)
//      .add("Notes", StringType, true)
// schema with custom format
//    val df_with_schema = spark.read.format("csv")
//      .option("header", "true")
//      .schema(schema)
//      .load("src/main/resources/zipcodes.csv")

    var df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true"))
      .csv("/home/ayushchatur/sales_data_sample.csv")
    val rdd = df.rdd
    df.printSchema()

//    df.show()
    val trimColumns = df.schema.fields.filter(_.dataType.isInstanceOf[StringType])
    trimColumns.foreach(f => {
      df = df.withColumn(f.name, trim(col(f.name)))
    })
//    df.foreach( row  => {
//
//    } )
    df.filter(lower(df.col("STATUS")).equalTo("shipped"))
      .groupBy("YEAR_ID", "PRODUCTLINE")

      .avg("SALES")
      .orderBy("YEAR_ID", "PRODUCTLINE")
//      .groupBy("")
      .show(false)



//    println("total coutn: " + c)
    // convert columns -> ORDERDATE
    // status ->enum ?
    // phone number -> string
    // convert status -> to upper (full proofing)
  }
}
