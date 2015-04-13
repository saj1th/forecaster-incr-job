package org.bitbucket.saj1th.forecasterincr

import java.util.Date

import com.datastax.spark.connector.{SomeColumns, _}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{L1Updater, SimpleUpdater, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.LocalDate
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}


/**
 * Enum for regularization params
 */
object RegularizationType extends Enumeration {
  type RegularizationType = Value
  val NONE, L1, L2 = Value
}

import org.bitbucket.saj1th.forecasterincr.RegularizationType._

/**
 * command line params
 */
case class Params(
                   data: String = null,
                   numIterations: Int = 100,
                   stepSize: Double = 1,
                   regType: RegularizationType = L2,
                   regParam: Double = 0.01,
                   master: String = "local[3]",
                   predStart: String = "2015-01-01",
                   predEnd: String = "2015-12-31",
                   modelSavePath: String = "./",
                   sparkExecutor: String = "",
                   cassandraHost: String = "127.0.0.1")

/**
 * ForecastJob incrementally updates the
 * sales forecast
 */
object ForecastJob extends SparkJob with Logging {
  def main(args: Array[String]) {

    var params = Params()
    val config = ConfigFactory.parseString("")

    var chost = Try(config.getString("cassandrahost"))
      .map(x => config.getString("cassandrahost"))
      .getOrElse(params.cassandraHost)

    val conf = new SparkConf()
      .setAppName(s"Forecaster Incrementer with $params")
      .setMaster(params.master)
      .set("spark.executor.memory", "1g")
      .set("spark.hadoop.validateOutputSpecs", "false") //TODO: handle model overwrites elegantly
      .set("spark.cassandra.connection.host", params.cassandraHost)

    logInfo("running forecaster with" + params)
    val sc = new SparkContext(conf)

    val results = runJob(sc, config)
    println("OK")
  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    val data = sc.textFile(config.getString("input.data")).map(_.split(","))
    val params = Params() //TODO: fix Params() & Config mixup

    // Aggregate number of sales per day per product
    // count over GROUP BY (sku +':'+date)
    val dailyVolume = data.map(r => (r(0).concat(":").concat(r(1)), 1))
      .reduceByKey((x, y) => x + y)
      .map(r => parseVolume(r._1, r._2))
      .persist()

    // Aggregate sale amount per day per product
    // sum of sales over GROUP BY (sku +':'+date)
    val dailySale = data.map(r => (r(0).concat(":").concat(r(1)), r(2).toFloat))
      .reduceByKey((x, y) => x + y)
      .map(r => parseSale(r._1, r._2))
      .persist()

    //Regularization Type
    val updater = params.regType match {
      case NONE => new SimpleUpdater()
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(params.numIterations)
      .setStepSize(params.stepSize)
      .setUpdater(updater)
      .setRegParam(params.regParam)


    var volumeModels: Map[String, LinearRegressionModel] = Map()
    var saleModels: Map[String, LinearRegressionModel] = Map()
    var scalerModels: Map[String, StandardScalerModel] = Map()
    // Lets load the old models from HDFS
    try {
      for (sku <- ProductData.skus) {
        volumeModels += (sku -> sc.objectFile[LinearRegressionModel](params.modelSavePath + "/volume." + sku + ".model").first())
        saleModels += (sku -> sc.objectFile[LinearRegressionModel](params.modelSavePath + "/sale." + sku + ".model").first())

      }
    } catch {
      case e: Exception => {
        //TODO: Fix error handling
        logError("Failed to load models from:" + params.modelSavePath)
        logError(s"${e.getMessage}")
      }
    }

    logInfo("training models")
    for (sku <- ProductData.skus) {
      //TODO: Need defensive checks
      trainVolumeModel(sku, dailyVolume, algorithm, volumeModels(sku)) match {
        case (Success((volModel, scalerModel))) =>
          volumeModels += (sku -> volModel)
          scalerModels += (sku -> scalerModel)

          //scalerModel needs to be calculated only once
          //TODO: Need defensive checks
          trainSaleModel(sku, dailySale, algorithm, scalerModel, saleModels(sku)) match {
            case (Success((saleModel))) =>
              saleModels += (sku -> saleModel)
            case Failure(ex) =>
              logError("Failed to train sale model for sku:" + sku)
              logError(s"${ex.getMessage}")
          }
        case Failure(ex) =>
          logError("Failed to train volume model for sku:" + sku)
          logError(s"${ex.getMessage}")
      }
    }

    // Do prediction
    logInfo("do prediction")
    val predictions = predictFuture(volumeModels, saleModels, scalerModels, params.predStart, params.predEnd)


    // Save to Cassandra
    logInfo("saving predictions to cassandra")
    val predictionsRdd = sc.parallelize(predictions)
    predictionsRdd.saveToCassandra("forecaster", "predictions", SomeColumns("sku", "date", "volume", "sale"))


    //Save models to HDFS
    logInfo("saving models to HDFS")
    //    persistModels(volumeModels, saleModels, scalerModels, sc, params.modelSavePath)

    logInfo("done!")
    sc.stop()


  }

  // Trains the Volume Model
  def trainVolumeModel(sku: String,
                       volumes: RDD[Volume],
                       algorithm: LinearRegressionWithSGD,
                       volModel: LinearRegressionModel): Try[(LinearRegressionModel, StandardScalerModel)] = {
    try {
      //Create labeled data
      val labelData = volumes
        .filter(row => row.sku == sku)
        .map { row => LabeledPoint(row.volume, Vectors.dense(row.year, row.month, row.day)) }

      //Feature scaling to standardize the range of independent variables
      val scaler = new StandardScaler(withMean = true, withStd = true).fit(labelData.map(x => x.features))
      val scaledData = labelData
        .map { data => LabeledPoint(data.label, scaler.transform(Vectors.dense(data.features.toArray))) }
        .cache()

      //Train the algo with initial weights from previous runs
      val model = algorithm.run(scaledData, volModel.weights)
      Success(model, scaler)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  // Trains the Sale Model
  def trainSaleModel(sku: String,
                     sales: RDD[Sale],
                     algorithm: LinearRegressionWithSGD,
                     scaler: StandardScalerModel,
                     saleModel: LinearRegressionModel): Try[LinearRegressionModel] = {

    try {
      //Create labeled data
      val labelData = sales
        .filter(row => row.sku == sku)
        .map { row => LabeledPoint(row.sale, Vectors.dense(row.year, row.month, row.day)) }
      //Feature scaling
      val scaledData = labelData
        .map { data => LabeledPoint(data.label, scaler.transform(Vectors.dense(data.features.toArray))) }
        .cache()

      //Train the algo
      val model = algorithm.run(scaledData, saleModel.weights)
      Success(model)
    } catch {
      case e: Exception => Failure(e)
    }

  }


  def parseVolume(x: String, y: Int) = {
    //split sku and date
    val split = x.split(':')
    //Split to year, month and day
    val dtSplit = split(1).split('-')
    Volume(split(0), dtSplit(0).toInt, dtSplit(1).toInt, dtSplit(2).toInt, y)
  }

  def parseSale(x: String, y: Float) = {
    //split sku and date
    val split = x.split(':')
    //Split to year, month and day
    val dtSplit = split(1).split('-')
    Sale(split(0), dtSplit(0).toInt, dtSplit(1).toInt, dtSplit(2).toInt, y)
  }

  // Perform prediction
  def predictFuture(volumeModels: Map[String, LinearRegressionModel],
                    saleModels: Map[String, LinearRegressionModel],
                    scalerModels: Map[String, StandardScalerModel],
                    predStart: String,
                    predEnd: String): ArrayBuffer[Prediction] = {
    //Get the day iterator
    val itr = dayStream(new LocalDate(predStart), new LocalDate(predEnd))
    var predictions = new ArrayBuffer[Prediction]()
    for (sku <- ProductData.skus) {
      for (dt <- itr) {
        predictions += Prediction(
          sku,
          dt.toDate,
          volumeModels(sku)
            .predict(scalerModels(sku)
            .transform(Vectors.dense(dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth))).toLong,
          saleModels(sku)
            .predict(scalerModels(sku)
            .transform(Vectors.dense(dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth))).toLong
        )
      }
    }
    predictions
  }

  // Iterate over date ranges
  def dayStream(start: LocalDate, end: LocalDate) = Stream.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end)

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.data"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("Please provice input data"))
  }

  //Persist Model Files
  def persistModels(volumeModels: Map[String, LinearRegressionModel],
                    saleModels: Map[String, LinearRegressionModel],
                    scalerModels: Map[String, StandardScalerModel],
                    sc: SparkContext,
                    path: String) = {
    try {
      //Save volume models
      for ((sku, model) <- volumeModels) {
        sc.parallelize(Seq(model), 1).saveAsObjectFile(path + "/volume." + sku + ".model")
      }
      //Save sale models
      for ((sku, model) <- saleModels) {
        sc.parallelize(Seq(model), 1).saveAsObjectFile(path + "/sale." + sku + ".model")
      }
      //Save scaler models
      for ((sku, model) <- scalerModels) {
        sc.parallelize(Seq(model), 1).saveAsObjectFile(path + "/scaler." + sku + ".model")
      }
    } catch {
      case e: Exception => {
        logError("Failed to save models to:" + path)
        logError(s"${e.getMessage}")
      }
    }
  }

  def checkMSE(model: LinearRegressionModel, testData: RDD[LabeledPoint]) = {
    //determine how well the model predicts the test data
    //measures the average of the squares of the "errors"
    val valsAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val power = valsAndPreds.map {
      case (v, p) => math.pow((v - p), 2)
    }

    // Mean Square Error
    val MSE = power.reduce((a, b) => a + b) / power.count()
    println("Model: " + model.weights)
    println("Mean Square Error: " + MSE)
  }
}


case class Prediction(sku: String, date: Date, volume: Long, sale: Long)
case class Sale(sku: String, year: Int, month: Int, day: Int, sale: Float)
case class Volume(sku: String, year: Int, month: Int, day: Int, volume: Int)
