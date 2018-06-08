val universe = spark.sqlContext.sql("select * from kt6.syn_universe")

val imps = spark.sqlContext.sql("select mob_ban from kt6.syn_tv_imps").distinct.withColumnRenamed("mob_ban", "ban")

val j1 = universe.join(imps, universe("mob_ban") === imps("ban"), "left_outer")

val j2 = j1.withColumn("exposedFlag", when($"ban".isNull or $"ban" === "", lit("no")).otherwise(lit("yes"))).drop("ban")

val response = spark.sqlContext.sql("select mob_ban, start_time, end_time, latitude, longitude from kt6.syn_response_j").withColumnRenamed("mob_ban", "ban")

val j3 = j2.join(response, j2("mob_ban") === response("ban"), "left_outer").drop(response("ban")).drop("mob_ban")

j3.write.format("csv").option("header","true").save("/apps/advertising/audience_measurement/kt6238/mapd/working_data/for_display.csv")
