import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("connectedComponent")
//    conf.setMaster("spark://hadoop005:7077")
//    conf.setJars(Array[String]("out/artifacts/ConnectedComponents_jar/SparkClient.jar"))
    conf.set("spark.driver.memory","32g")
    conf.set("spark.executor.cores","8")//10
    conf.set("spark.executor.memory","32g")//60
    conf.set("spark.yarn.executor.memoryOverhead","32768")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
//    hiveContext.sql("insert overwrite table graphx.pm select 0,0,phone,mail from nimbuzz.nimbuzz_user2")
//    hiveContext.sql("insert overwrite table graphx.phones select 1,phone from graphx.pm where phone<>'null'")
//    hiveContext.sql("insert overwrite table graphx.mails select 1,mail from graphx.pm where mail<>'null'")
//    hiveContext.sql("insert overwrite table graphx.phones select row_number() over (order by phone),phone from graphx.phones group by phone")
//    hiveContext.sql("insert overwrite table graphx.mail select row_number() over (order by mail) + t.p_max,mail from graphx.mail cross join (select max(id) p_max from graphx.phones) t group by mail ")
//
//    hiveContext.sql("insert overwrite table graphx.pm select id,0,phone,mail from graphx.pm a left join graphx.phones b on a.phone=b.phone")
//    hiveContext.sql("insert overwrite table graphx.pm select pl,id,phone,mail from graphx.pm a left join graphx.mails b on a.mail=b.mail")


    val phone = hiveContext.sql("select id,phone from graphx.phones")
    val phone_rdd = phone.rdd.map(f=> (f.getLong(0),(f.getString(1)))).cache()

    val mail = hiveContext.sql("select id,mail from graphx.mails")
    val mail_rdd = mail.rdd.map(f=> (f.getLong(0),(f.getString(1)))).cache()

    //val e = hiveContext.sql("select id_from,id_to,name from sparkgraphx.mid")
    val e = hiveContext.sql("select pl,ml from graphx.pm where pl is not null and ml is not null " )
    val erdd = e.rdd.map(f=>Edge(f.getLong(0),f.getLong(1),""))
    val graph: Graph[String, String] = Graph(phone_rdd.union(mail_rdd),erdd).partitionBy(PartitionStrategy.CanonicalRandomVertexCut)  //构建图

    //graph.triplets.foreach(x=>println(x.srcAttr))
    val graph2 = graph.mapVertices((vid:VertexId,rowkey:String) =>vid)
    val initialMessage = Long.MaxValue   //初始化顶点
    val maxIterations:Int = Int.MaxValue   //最大迭代次数
    val vgFun = (id:VertexId,attr:Long,msg:Long) =>Math.min(attr,msg)  //顶点初始化函数

    //消息传递函数
    val sendMessageFun = (edge:EdgeTriplet[Long,String]) => {
      println("-------------srcAttr: "+edge.srcAttr.toString)
      println("------------dstAttr: "+edge.dstAttr.toString)

      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    //消息合并函数
    val combineFun = (a:Long,b:Long) => math.min(a,b)
    val cc = graph2.pregel(initialMessage,maxIterations,EdgeDirection.Either)(vgFun,sendMessageFun,combineFun).cache()
    val result_phone = cc.vertices.join(phone_rdd).map{case(id,(id2,rowkey))=>Group(id,id2,rowkey)}
    val result_mail = cc.vertices.join(mail_rdd).map{case(id,(id2,rowkey))=>Group(id,id2,rowkey)}
    result_phone.toDF().registerTempTable("phoneResult")
    hiveContext.sql("insert overwrite table graphx.result_phone select * from phoneResult")
//    result_mail.take(10).foreach(println)
    result_mail.toDF.registerTempTable("mailResult")
    hiveContext.sql("insert overwrite table graphx.result_mail select * from mailResult")
    sc.stop()
  }
}
