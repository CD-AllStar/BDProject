import bd.util.ANSJUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object CityRelSplit {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Rel-Split")
//    conf.setMaster("spark://hadoop005:7077")
//    conf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar","out/artifacts/SparkClient_jar/ZHConverter.jar"))
//    conf.set("spark.executor.cores","4")
//    conf.set("spark.executor.memory","8g")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("use spark")
    val city_names = hiveContext.sql("select uid,name,source_id from beetalks.src_name_new where source_id<>'Unknown' and source_id<>'Unknow'")
    val city_name = city_names.rdd.map(x=>((x.getString(0),x.getString(2)),x.getString(1)))
//        .filter(x=>IK.isChinese(x._2))
       .map(x=>(x._1,ANSJUtil.getInstance.toAnalysis(x._2))).flatMapValues(_.split(","))
      .filter(x=>x._2.split("\t").length==3)
      .filter(x=>x._2.split("\t")(1).startsWith("关系"))
      .map(x=>RelationShip(x._1._2,x._1._1,x._2.split("\t")(0),x._2.split("\t")(1),x._2.split("\t")(2)))

//    city_name.take(10).foreach(println)

    city_name.toDF.registerTempTable("RelSplit")
//
    hiveContext.sql("insert overwrite table rel_split select u_uid,c_uid,ship,ship_type,synonym from RelSplit")
    sc.stop()
  }
}
