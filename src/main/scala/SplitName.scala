import bd.util.{ANSJUtil, LanguageUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object SplitName {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Split-Name")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val names = hiveContext.sql("select uid,name,source_id from beetalks.src_name_new")
    val splited_name = names.rdd.map(x => ((x.getString(0), x.getString(1), x.getString(2)), x.getString(1)))
      .map(x =>
        if (LanguageUtil.containChinese(x._2)) {
          (x._1, x._2.replaceAll(" ", ""))
        }
        else {
          x
        })
      .map(x => (x._1, ANSJUtil.getInstance.toAnalysis(x._2))).flatMapValues(_.split(","))
      .filter(x => x._2.split("\t").length == 3).filter(x=>x._2.split("\t")(2).length>1)
      .map(x => SplitedName(x._1._1, x._1._2, x._1._3, x._2.split("\t")(0), x._2.split("\t")(1), x._2.split("\t")(2)))

    splited_name.toDF.registerTempTable("SplitedName")

    hiveContext.sql("insert overwrite table spark.splited_name select distinct * from SplitedName")
    sc.stop()
  }
}
