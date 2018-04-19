import bd.util.ANSJUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object CityNameSplit {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("CityNameSplit")
//    conf.setMaster("spark://hadoop005:7077")
//    conf.setJars(Array[String]("out/artifacts/CityNameSplit_jar/SparkClient.jar","out/artifacts/CityNameSplit_jar/ZHConverter.jar"))
//    conf.set("spark.executor.cores","4")
//    conf.set("spark.executor.memory","8g")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("use spark")
    val city_names = hiveContext.sql("select uid,country,province,city,name,source_id from city_name")
    val city_name = city_names.rdd.map(x=>((x.getString(0),x.getString(1),x.getString(2),x.getString(3),x.getString(5)),x.getString(4)))
      .map(x=>(x._1,ANSJUtil.getInstance.toAnalysis(x._2))).flatMapValues(_.split(","))
      .filter(x=>x._2.split("\t").length==3)
      .filter(x=>x._2.split("\t")(0).length>1)
      .map(x=>CityWord(x._1._1,x._1._2,x._1._3,x._1._4,x._2.split("\t")(0),x._2.split("\t")(1),x._1._5,x._2.split("\t")(2)))


//    city_name.take(100).foreach(println)
    city_name.toDF.registerTempTable("CityNameSplit")
//
    hiveContext.sql("insert overwrite table city_name_split select uid,country,province,city,word,source,source_id,synonym from CityNameSplit")

  }
}
