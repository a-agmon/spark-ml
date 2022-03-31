package com.synamedia.analytics


import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import ai.catboost.spark._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.DoubleType

object TestMe extends App{

  val spark = SparkSession.builder()
    .appName("catboost Prediction")
    .config("spark.master", "local")
    .config("spark.driver.host","127.0.0.1")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

  import spark.implicits._

  val fileUrl:String = "/Users/aagmon/work/synamedia/dataset.csv"
  val modelPath:String = "/Users/aagmon/work/synamedia/catboost_model.cbm"

  val rawFeaturesDF = spark.read
    .format("csv")
    .option("header", true)
    .load(fileUrl)

  rawFeaturesDF.show()
  rawFeaturesDF.printSchema()

  val castQueryColumns:Seq[Column] = rawFeaturesDF
    .columns.map { col => rawFeaturesDF(col).cast(DoubleType).as(col)}

  val transformedDF = rawFeaturesDF.select(castQueryColumns:_*)
  transformedDF.show()

  val assembler = new VectorAssembler()
    .setInputCols(transformedDF.columns)
    .setOutputCol("features")

  val predictionFeaturesDF:DataFrame = assembler.transform(transformedDF).selectExpr("features")
  val predictionPool = new Pool(predictionFeaturesDF)
  val model = CatBoostClassificationModel.loadNativeModel(modelPath)
  val predictions = model.transform(predictionPool.data)
  predictions.show()

}
