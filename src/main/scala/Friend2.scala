import java.util
import java.util
import util.{ArrayList, List, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import _root_.jdbc.Mysql


object Friend2 {
  def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setMaster("spark://hadoop002:7077")
        conf.setAppName("2Friend")
        conf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar"))
        conf.set("spark.executor.cores","4")
        conf.set("spark.executor.memory","16g")

        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
    import hiveContext._
        import hiveContext.implicits._
        hiveContext.sql("use sparkgraphx")

    val jdbcUtils = new Mysql()
    jdbcUtils.getConnection
    val sql2: String = "select distinct uid from focusmap where status = -1"
    val sql3: String = "update focusmap set status = 0 where uid = ? "
    val sql4: String = "update focusmap set status = 1 where uid = ? "
    val list: util.List[util.Map[String, AnyRef]] = jdbcUtils.findModeResult(sql2, null)
    import scala.collection.JavaConversions._
    println(list)
    for (result <- list) {
      val params: util.List[AnyRef] = new util.ArrayList[AnyRef]
      var uid = result.get("uid").toString
      println(uid)
      params.add(uid)
//      jdbcUtils.updateByPreparedStatement(sql3, params)
      hiveContext.sql("insert into table tmp1 select a.uid,b.uid,a.c_uid,a.name,b.name from beetalks.src_contact a join beetalks.src_contact b on a.c_uid=b.c_uid where a.uid='6ecc1f6a636a4274be780a1193a2cc3b' and a.uid<>b.uid and a.c_uid<>b.uid")

      hiveContext.sql("insert into table 2friend select uid1,uid2,collect_set(contact_ws('\t',c_uid,name1,name2) from tmp1 group by uid1,uid2")
//      jdbcUtils.updateByPreparedStatement(sql4,params)
    }


  }
}
