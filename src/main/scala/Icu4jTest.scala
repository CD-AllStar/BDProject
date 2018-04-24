import bd.util.Icu4j
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object Icu4jTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
//    conf.setMaster("spark://hadoop005:7077")
    conf.setAppName("Icu4j-TEST")
//    conf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar","out/artifacts/SparkClient_jar/ZHConverter.jar"))
//    conf.set("spark.executor.cores","2")
//    conf.set("spark.executor.memory","4g")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
//    val fsConf = new Configuration
//    val fs= FileSystem.get(fsConf)
//    val text = sc.textFile("/user/arango/20171122/nodes")
    hiveContext.sql("use spark")
    val city_names = hiveContext.sql("select uid,country,province,city,name,source_id from city_name")
    val city_name = city_names.rdd.map(x=>((x.getString(0),x.getString(1),x.getString(2),x.getString(3),x.getString(5)),x.getString(4)))
      .map(x=>(x._1,Icu4j.toAnalysis(x._2))).flatMapValues(_.split(","))
      .filter(x=>(x._2.length>1 && !x._2.matches("[0-9]*")))
      .map(x=>CityWord(x._1._1,x._1._2,x._1._3,x._1._4,x._2,"test",x._1._5,"test"))
    city_name.toDF.registerTempTable("CityNameSplit")

    hiveContext.sql("insert overwrite table city_name_test select uid,country,province,city,word,source,source_id from CityNameSplit")


  }
}
