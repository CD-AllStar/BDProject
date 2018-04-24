import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Family {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Family")
//    conf.setMaster("spark://hadoop005:7077")
//    conf.setJars(Array[String]("out/artifacts/ConnectedComponents_jar/SparkClient.jar"))
//    conf.set("spark.driver.memory","32g")
//    conf.set("spark.executor.cores","8")//10
//    conf.set("spark.executor.memory","32g")//60
//    conf.set("spark.yarn.executor.memoryOverhead","32768")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val vertext = hiveContext.sql("select id,uid from spark.family_vertex")
    val vertext_rdd = vertext.rdd.map(f=> (f.getLong(0),(f.getString(1)))).cache()


    //val e = hiveContext.sql("select id_from,id_to,name from sparkgraphx.mid")
    val edge = hiveContext.sql("select src_id,dst_id from spark.family_edge " )
    val edge_rdd = edge.rdd.map(f=>Edge(f.getLong(0),f.getLong(1),"family"))
    val graph: Graph[String, String] = Graph(vertext_rdd,edge_rdd).partitionBy(PartitionStrategy.CanonicalRandomVertexCut)  //构建图

    val cc = graph.connectedComponents().cache()
    val result = cc.vertices.join(vertext_rdd).map{case(id,(id2,rowkey))=>Group(id,id2.toLong,rowkey)}
    result.toDF().registerTempTable("Result")
    hiveContext.sql("insert overwrite table spark.family_result_vertex select * from Result")
    sc.stop()
  }
}
