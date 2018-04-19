import bd.util.ANSJUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object NameRecognize {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("NameRecognize-CN")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("use beetalks")
    val names = hiveContext.sql("select uid,name from src_name")
    val name = names.rdd.map(x=>(x.getString(0),x.getString(1)))
//        .filter(x=>IK.isChinese(x._2))
      .map(x=>(x._1,x._2.replaceAll(" ","")))
      .map(x=>(x._1,ANSJUtil.getInstance().toAnalysis(x._2)))
      .flatMapValues(_.split(","))
      .filter(x=>x._2.split("\t").length == 3)
      .filter(x=>x._2.split("\t")(1).contains("nr")&&x._2.split("\t")(0).length>1)
      .map(x=>RealName(x._1,x._2.split("\t")(0)))

//    name.take(100).foreach(println)
    name.toDF.registerTempTable("NameRecognize")
//
    hiveContext.sql("insert overwrite table spark.real_name select uid,name from NameRecognize")

  }
}
