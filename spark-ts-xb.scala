// add to your SBT 
//  "org.apache.spark" %% "spark-mllib" % "3.3.2",
//  "ml.dmlc" %% "xgboost4j" % "1.7.3",

package com.aagmon.examples

import TimeSeriesAnalyzer.{getMasterConfig, getS3ReadingConf}

import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}


import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

case class FeaturesRecord(key: String, ts:Timestamp, features: Seq[Float], label: Float)
case class PredictionResult(key: String, ts:Timestamp, label: Float, prediction: Float, ratio: Float)

object MultipleTSForecaster {

  implicit class DMatrixConverter(seq: Seq[FeaturesRecord]) {
    def toDMatrix: DMatrix = {
      val labeledPoints = seq.map { case FeaturesRecord(_, _, features, label) =>
        LabeledPoint(label, features.size, null, features.toArray)
      }
      new DMatrix(labeledPoints.iterator)
    }
  }

  private def getSparkSession: Try[SparkSession] = Try {
    SparkSession.builder()
      .config(getMasterConfig)
      .config(getS3ReadingConf)
      .appName("TimeSeriesAnalyzer")
      .getOrCreate()
  }

  private def readDataSet(spark: SparkSession): Try[DataFrame] = Try {
    import spark.implicits._
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/data_samples_v2.csv")
      .where($"lag6".isNotNull)
  }

  private def getFeaturesDataFrame(df: DataFrame, sparkSession: SparkSession): Try[Dataset[FeaturesRecord]] = Try {
    import sparkSession.implicits._
    df.map(row => {
      val key = row.getAs[String]("app_id")
      val label = row.getAs[Int]("installs").toFloat
      val ts = row.getAs[Timestamp]("event_hour")
      val dayOfWeek = row.getAs[Int]("day_of_week").toFloat
      val hourOfDay = row.getAs[Int]("hour_of_day").toFloat
      var features = Seq(dayOfWeek, hourOfDay) ++
        (1 to (6)).map(index => row.getAs[Int](s"lag${index}").toFloat)
      (key, ts, features, label)
    }).toDF("key", "ts", "features", "label")
      .as[FeaturesRecord]
  }

  private def getForecastDatasets(app_id: String, recordsIterator: Seq[FeaturesRecord]): Try[(Seq[FeaturesRecord], Seq[FeaturesRecord])] = Try {
    val dataPoints = recordsIterator.sortBy(_.ts.getTime)
    // im ignoring the last observation as they may be partial
    val trainSeq = dataPoints.slice(0, dataPoints.length - 3)
    val actualValSeq = dataPoints.slice(dataPoints.length - 3, dataPoints.length - 1)
    (trainSeq, actualValSeq)
  }

  private def trainXGBBooster(trainSeq: Seq[FeaturesRecord]): Try[Booster] = Try {
    XGBoost.train(trainSeq.toDMatrix, Map("eta" -> 0.1f, "max_depth" -> 4, "objective" -> "reg:squarederror"), 50)
  }

  private def predictXGBBooster(app_id: String, booster: Booster, predictSeq: Seq[FeaturesRecord]): Try[Seq[PredictionResult]] = Try {
    val forecastedVal = booster.predict(predictSeq.toDMatrix)
    predictSeq.zip(forecastedVal).map { case (FeaturesRecord(_, ts, _, label), forecast) =>
      PredictionResult(app_id, ts, label, forecast(0), label / forecast(0))
    }
  }

  def predict(appId:String, recordsIter:Iterator[FeaturesRecord]): Seq[PredictionResult] = {
    val predDF = for {
      (trainSeq, actualValSeq) <- getForecastDatasets(appId, recordsIter.toSeq)
      booster <- trainXGBBooster(trainSeq)
      forecastedVal <- predictXGBBooster(appId, booster, actualValSeq)
    } yield forecastedVal
    predDF match {
      case Success(predictionResults) => predictionResults
      case Failure(exc) => throw new RuntimeException(s"prediction failed due to $exc")
    }
  }


  private def runPredictionFlow(): Try[Dataset[PredictionResult]] = {
    val sparkSession = getSparkSession.getOrElse(throw new Exception("failed to get spark session"))
    import sparkSession.implicits._
    for {
      rawDF <- readDataSet(sparkSession)
      featuresDS <- getFeaturesDataFrame(rawDF, sparkSession)
      predictions <- Try{featuresDS.groupByKey(_.key).flatMapGroups(predict)}
    } yield predictions
  }

  def main(args: Array[String]): Unit = {
    val prediction = runPredictionFlow()
    prediction match {
      case Failure(exception) => println(s"prediction flow failed due to $exception")
      case Success(df) => df.show(10)
    }
  }

}


