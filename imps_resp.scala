val imps = spark.sqlContext.sql("select * from kt6.syn_tv_imps").distinct

val response = spark.sqlContext.sql("select * from am.mcdonald_visits").select("mob_ban", "start_time", "end_time", "latitude", "longitude").distinct.withColumnRenamed("mob_ban", "ban")

// val response_count = response.groupBy("ban").count().withColumnRenamed("count", "cnt").show(false)

val diff_sec = col("end_time").cast("long") - col("start_time").cast("long")

val new_resp = response.withColumn("dwell_ts_m", diff_sec/ 60D)

# select dwell between five and 60 minutes.
val sel_resp = new_resp.filter($"dwell_ts_m" > 5 && $"dwell_ts_m" < 60).select("ban").distinct




val j1 = imps.join(sel_resp, imps("mob_ban") === sel_resp("ban"), "left_outer")

# create target

val j2 = j1.withColumn("response_label", when($"ban".isNull or $"ban" === "", lit("FALSE")).otherwise(lit("TRUE"))).drop("ban", "ts")

val j2x = j2.na.drop(how = "any")

/*
CREATE external TABLE kt6.syn_imps_resp (
mob_ban string,
network string,
daypart string,
dma string,
estimated_hh_income int,
household_size int,
number_adults int,
number_children int,
response_label boolean
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/advertising/audience_measurement/mapd/db/syn_imps_resp';
*/
import org.apache.spark.sql.types.IntegerType
val j3 = j2.selectExpr("mob_ban", "network", "daypart", "dma", "cast(estimated_hh_income as int) income", "cast(household_size as int) household_size", "cast(number_adults as int) numoer_adults", "cast(number_children as int) number_children", "cast(response_label as boolean) response_label")


j3.distinct.write.mode("overwrite").option("path", "/apps/advertising/audience_measurement/mapd/db/syn_imps_resp").saveAsTable("kt6.syn_imps_resp")

import java.io.File

import _root_.hex.genmodel.utils.DistributionFamily
import _root_.hex.deeplearning.DeepLearning
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
import DeepLearningParameters.Activation
import DeepLearningParameters.Loss
import org.apache.spark.SparkFiles
import org.apache.spark.h2o.{DoubleHolder, H2OContext, H2OFrame}
import org.apache.spark.sql.Dataset
import water.support.{H2OFrameSupport, SparkContextSupport, SparkSessionSupport}
import org.apache.spark.examples.h2o.AirlinesParse
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.rand



// Run H2O cluster inside Spark cluster
val h2oContext = H2OContext.getOrCreate(sc)
import h2oContext._
import h2oContext.implicits._


val good_data = spark.sqlContext.sql("select * from kt6.syn_imps_resp").drop("mob_ban").na.drop(how = "any")


val good_data1 = good_data('network, 'daypart, 'dma, 'income, 'household_size, 'numoer_adults, 'number_children, 'response_label)
H2OFrameSupport.withLockAndUpdate(good_data){ fr => fr.replace(fr.numCols()-1, fr.lastVec().toCategoricalVec)}




// try h2o
// split into three sets.
val newDF = good_data.randomSplit(Array(0.6, 0.2, 0.2))
val (train, test, validation) = (newDF(0), newDF(1), newDF(2))
val train1 = train('network, 'daypart, 'dma, 'income, 'household_size, 'numoer_adults,
     'number_children, 'response_label)
H2OFrameSupport.withLockAndUpdate(train1){ fr => fr.replace(fr.numCols()-1, fr.lastVec().toCategoricalVec)}

val test1 = test('network, 'daypart, 'dma, 'income, 'household_size, 'numoer_adults,
     'number_children, 'response_label)
H2OFrameSupport.withLockAndUpdate(test1){ fr => fr.replace(fr.numCols()-1, fr.lastVec().toCategoricalVec)}

val validation1 = validation('network, 'daypart, 'dma, 'income, 'household_size, 'numoer_adults,
     'number_children, 'response_label)
H2OFrameSupport.withLockAndUpdate(validation1){ fr => fr.replace(fr.numCols()-1, fr.lastVec().toCategoricalVec)}
// make H2OFrame
val train_h2o : H2OFrame = h2oContext.asH2OFrame(train1)
val test_h2o  : H2OFrame = h2oContext.asH2OFrame(test1)
val valid_h2o : H2OFrame = h2oContext.asH2OFrame(validation1)


val hidden: Array[Int] = Array(500, 400, 300, 200)
val dlParams = new DeepLearningParameters()
dlParams._train = new H2OFrame(train_h2o)
dlParams._valid = new H2OFrame(test_h2o)
dlParams._response_column = "response_label"
dlParams._epochs = 10
dlParams._l1 = 0.0001
dlParams._l2 = 0.0001
dlParams._activation = Activation.RectifierWithDropout
dlParams._hidden = hidden
dlParams._distribution = DistributionFamily.multinomial
val dl = new DeepLearning(dlParams)
val model = dl.trainModel.get
model




import _root_.hex.tree.drf.DRFModel.DRFParameters
import _root_.hex.tree.drf.DRFModel
import _root_.hex.tree.drf.DRF
val drfParams = new DRFParameters()
drfParams._train = train_h2o
drfParams._response_column = 'response_label
drfParams._ntrees = 500
drfParams._balance_classes = true
drfParams._max_after_balance_size = 2.0f

val rf = new DRF(drfParams)
val model = rf.trainModel().get()

import _root_.hex.tree.gbm.GBMModel
import _root_.hex.{Model, ModelMetricsBinomial}


//  import h2oContext.implicits._
  import _root_.hex.tree.gbm.GBM
  import _root_.hex.tree.gbm.GBMModel.GBMParameters

  val gbmParams = new GBMParameters()
  gbmParams._train = train_h2o
  gbmParams._valid = test_h2o
  gbmParams._response_column = 'response_label
  gbmParams._ntrees = 500
  gbmParams._max_depth = 3
  gbmParams._distribution = DistributionFamily.bernoulli

  val gbm = new GBM(gbmParams)
  val model = gbm.trainModel.get
  model






  import water.MRTask
  import water.fvec.{Chunk, NewChunk, Vec}
  import water.parser.{BufferedString, ParseSetup}
  import water.support.{H2OFrameSupport, ModelMetricsSupport, SparkContextSupport, SparklingWaterApp}

  val trainMetrics = ModelMetricsSupport.binomialMM(model, train_h2o)
  val validMetrics = ModelMetricsSupport.binomialMM(model, valid_h2o)
