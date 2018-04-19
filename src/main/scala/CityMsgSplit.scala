import bd.util.ANSJUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object CityMsgSplit {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
//    conf.setMaster("spark://hadoop005:7077")
    conf.setAppName("MsgSplit")
//    conf.setJars(Array[String]("out/artifacts/CityMsgSplit_jar/SparkClient.jar","out/artifacts/CityMsgSplit_jar/ZHConverter.jar"))
//    conf.set("spark.executor.cores","4")
//    conf.set("spark.executor.memory","8g")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("use spark")
    val city_msgs = hiveContext.sql("select uid,msg_id,content,time,platform from src_msg")
    val city_msg = city_msgs.rdd.map(x=>((x.getString(0),x.getString(1),x.getLong(3),x.getString(4)),x.getString(2)))
      .map(x=>(x._1,ANSJUtil.getInstance.toAnalysis(x._2))).flatMapValues(_.split(","))
        .filter(x=>x._2.split("\t").length==3)
      .filter(x=>x._2.split("\t")(0).length>1)
      .map(x=>MsgWord(x._1._1,x._1._2,x._2.split("\t")(0),x._2.split("\t")(1),x._1._3,x._1._4,x._2.split("\t")(2)))

//    city_msg.take(10).foreach(println)


    city_msg.toDF().registerTempTable("MsgSplit")
    hiveContext.sql("insert overwrite table src_msg_split select uid,msg_id,word,source,time,platform,synonym from MsgSplit")

  }
}
