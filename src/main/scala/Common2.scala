import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object Common2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CommonFriends2")
//            conf.setMaster("spark://hadoop005:7077")
//            conf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar"))
//            conf.set("spark.executor.cores","4")
//            conf.set("spark.executor.memory","8g")


    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("use beetalks")
    val data = hiveContext.sql("select uid,c_uid from src_contact where uid<>c_uid group by uid,c_uid").rdd.repartition(2000).cache()
    val contacts = data.map(x=>(x.getString(1),x.getString(0))).cache()
    val becontacts = contacts.reduceByKey(_+","+_).cache()
    val common = contacts.join(becontacts).map(_._2).flatMapValues(_.split(",")).filter(x=>(!x._1.equals(x._2))).map(x=>(x,1)).reduceByKey(_+_)
      .map(x=>CommonFriends(x._1._1,x._1._2,x._2))
    common.take(10).foreach(println)

//    val common = contacts.flatMap{x=>
//      val results = new util.ArrayList[((String,String),Set[String])]()
//      val self = x._1
//      val friends = x._2.split(",").toSet
//      for(friend<-friends){
//        if(self<friend){
//          results.add(((self,friend),friends))
//        }else{
//          results.add(((friend,self),friends))
//        }
//      }
//      results.toArray()
//    }.map{x=>
//      val pairs = x.asInstanceOf[((String,String),Set[String])]
//      val selfAndFriend = pairs._1
//      val friends = pairs._2
//      (selfAndFriend,(1,friends))
//    }.reduceByKey((x,y)=>(x._1+y._1,x._2.&(y._2))).filter(x=>x._2._1==2).map(x=>CommonFriends(x._1._1,x._1._2,x._2._2.size))
//
    common.toDF().registerTempTable("CommonFriends")
//    common.take(10).foreach(println)
//
    hiveContext.sql("insert overwrite table score.common partition(date='20180403') select u_uid,c_uid,count from CommonFriends")


  }
}
