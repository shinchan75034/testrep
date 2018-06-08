import org.apache.spark.sql.functions._

val  response = spark.sqlContext.sql("select * from am.mcdonald_visits").select("mob_ban", "start_time", "end_time", "latitude", "longitude").distinct

val universe = spark.sqlContext.sql("select * from u76.study_universe")

val acx = spark.sqlContext.sql("select * from u76.acx_gold_1")

val j1 = response.join(universe, response("mob_ban")===universe("mob_ban")).drop(universe("mob_ban"))

val j2 = j1.join(acx, j1("mob_ban")===acx("mob_ban")).drop(acx("mob_ban"))

val j2r = j2.withColumn("rand_lat", (rand()-0.5)*1e-3).withColumn("rand_lon", (rand()-0.5)*1e-3)

val j2add = j2r.withColumn("new_lat", j2r("latitude") + j2r("rand_lat")).withColumn("new_lon", j2r("longitude") + j2r("rand_lon"))

val j3 = j2add.drop("rand_lat", "rand_lon", "latitude", "longitude")

val j3new = j3.withColumnRenamed("new_lat", "latitude").withColumnRenamed("new_lon", "longitude").select("mob_ban","start_time","end_time","latitude","longitude","dma","estimated_hh_income","household_size","number_adults","number_children")

/*
CREATE external TABLE kt6.syn_response_j (
mob_ban string,
start_time timestamp,
end_time timestamp,
latitude double,
longitude double,
dma string,
estimated_hh_income int,
household_size int,
number_adults int,
number_children int
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/advertising/audience_measurement/mapd/db/syn_response_j';
*/

j3new.distinct.write.mode("overwrite").option("path", "/apps/advertising/audience_measurement/mapd/db/syn_response_j").saveAsTable("kt6.syn_response_j")

val j5 = universe.join(acx, universe("mob_ban")===acx("mob_ban")).drop(acx("mob_ban"))

/*
CREATE external TABLE kt6.syn_universe (
mob_ban string,
dma string,
estimated_hh_income int,
household_size int,
number_adults int,
number_children int
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/advertising/audience_measurement/mapd/db/syn_universe';
*/

j5.distinct.write.mode("overwrite").option("path", "/apps/advertising/audience_measurement/mapd/db/syn_universe").saveAsTable("kt6.syn_universe")


val datRdd = sc.textFile("/apps/advertising/audience_measurement/u76/mutualexclusive/impr/exposure/tvonly").map(line => line.split("\\|"))
case class Dat(mob_ban: String = "", ts: String = "", network: String = "", daypart: String = "")

val tv = datRdd.map(f => Dat(f(0).toString, f(1).toString, f(2).toString, f(3).toString)).toDF()

val tvj1 = tv.join(universe, tv("mob_ban")===universe("mob_ban")).drop(universe("mob_ban"))

val tvj2 = tvj1.join(acx, tvj1("mob_ban")===acx("mob_ban")).drop(acx("mob_ban"))

/*
CREATE external TABLE kt6.syn_tv_imps (
mob_ban string,
ts timestamp,
network string,
daypart string,
dma string,
estimated_hh_income int,
household_size int,
number_adults int,
number_children int
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/advertising/audience_measurement/mapd/db/syn_tv_imps';
*/

tvj2.distinct.write.mode("overwrite").option("path", "/apps/advertising/audience_measurement/mapd/db/syn_tv_imps").saveAsTable("kt6.syn_tv_imps")
