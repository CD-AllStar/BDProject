import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext


object SSC {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("ShortestPath")
    conf.setMaster("spark://hadoop005:7077")
    conf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar"))
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")
//    conf.set("spark.driver.memory","8g")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
//    val myVertices = hiveContext.sql("select * from graphx.vertex").rdd.map(row=>(row.getLong(0),row.getString(1))).cache()
//    val myEdges = hiveContext.sql("select src_id,dst_id from graphx.edg where src_id<>dst_id").rdd.map(row=>Edge(row.getLong(0),row.getLong(1),1.0)).cache()

    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G"),(10L,"H"),(8L,"I")))
    val initialEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0),
      Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0),
      Edge(6L, 7L, 11.0),
      Edge(10L, 7L, 10.0),
      Edge(10L, 8L, 10.0)))
    val myEdges = initialEdges.filter(e => e.srcId != e.dstId)
      .flatMap(e => Array(e, Edge(e.dstId, e.srcId, e.attr)))
      .distinct() //去掉自循环边，有向图变为无向图，去除重复边
    val myGraph = Graph(myVertices, myEdges).cache()
//    val rank = myGraph.pageRank(0.01).vertices.map(x=>PageRank(x._1.toLong,x._2))
//    rank.toDF().registerTempTable("Rank")
//    hiveContext.sql("insert overwrite table score.page_rank select * from Rank")

//    val sourceId: VertexId = 84317865 // The ultimate source
//    // Initialize the graph such that all vertices except the root have distance infinity.
//
//    val initialGraph: Graph[(Double, List[VertexId]), Double] = myGraph.mapVertices((id, _) =>
//      if (id == sourceId) (0.0, List[VertexId](sourceId))
//      else (Double.PositiveInfinity, List[VertexId]()))
//
//    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), 6)(
//
//      // Vertex Program
//      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,
//
//      // Send Message
//      triplet => {
//        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
//          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
//        } else {
//          Iterator.empty
//        }
//      },
//      //Merge Message
//      (a, b) => if (a._1 < b._1) a else b)
//    println(sssp.vertices.collect.mkString("\n"))

//    println(ShortestPaths.run(myGraph, myVertices.map(x=>x._1).collect()).vertices.collect().mkString(","))

    //    println(dijkstra(myGraph, 3L).vertices.map(x => (x._1, x._2)).collect().mkString(" | "))
    //    println(prime(myGraph, 3L).vertices.map(x => (x._1, x._2)).collect().mkString(" | "))
        floydWarshall(myGraph).vertices.collect().foreach(println)
  }

  //单源最短路径
  def dijkstra[VD](g: Graph[VD, Double], origin: VertexId) = {
    //初始化，其中属性为（boolean, double，Long）类型，boolean用于标记是否访问过，double为顶点距离原点的距离，Long是上一个顶点的id
    var g2 = g.mapVertices((vid, _) => (false, if (vid == origin) 0 else Double.MaxValue, -1L))

    for (i <- 1L to g.vertices.count()) {
      //从没有访问过的顶点中找出距离原点最近的点
      val currentVertexId = g2.vertices.filter(!_._2._1).reduce((a, b) => if (a._2._2 < b._2._2) a else b)._1
      //更新currentVertexId邻接顶点的‘double’值
      val newDistances = g2.aggregateMessages[(Double, Long)](
        triplet => if (triplet.srcId == currentVertexId && !triplet.dstAttr._1) { //只给未确定的顶点发送消息
          triplet.sendToDst((triplet.srcAttr._2 + triplet.attr, triplet.srcId))
        },
        (x, y) => if (x._1 < y._1) x else y,
        TripletFields.All
      )
      //newDistances.foreach(x => println("currentVertexId\t"+currentVertexId+"\t->\t"+x))
      //更新图形
      g2 = g2.outerJoinVertices(newDistances) {
        case (vid, vd, Some(newSum)) => (vd._1 || vid == currentVertexId, math.min(vd._2, newSum._1), if (vd._2 <= newSum._1) vd._3 else newSum._2)
        case (vid, vd, None) => (vd._1 || vid == currentVertexId, vd._2, vd._3)
      }
      //g2.vertices.foreach(x => println("currentVertexId\t"+currentVertexId+"\t->\t"+x))
    }

    //g2
    g.outerJoinVertices(g2.vertices)((vid, srcAttr, dist) => (srcAttr, dist.getOrElse(false, Double.MaxValue, -1)._2, dist.getOrElse(false, Double.MaxValue, -1)._3))
  }

  //多源最短路径
  def floydWarshall[VD](g: Graph[VD, Double]) = {
    def mergeMaps(a: Map[VertexId, Double], b: Map[VertexId, Double]) = {
      (a.keySet ++ b.keySet).map { k => (k, math.min(a.getOrElse(k, Double.MaxValue), b.getOrElse(k, Double.MaxValue))) }.toMap
    }

    val N = g.vertices.count() //图顶点的个数
    var n = -1
    //初始化图
    var g2 = g.mapVertices((vid, _) => Map(vid -> 0.0))

    //当n = N*N时，退出循环。注：不难发现最终结果是一个实对称矩阵
    while (n < N * N) {
      val newVertices = g2.aggregateMessages[Map[VertexId, Double]](
        triplet => {
          val dstPlus = triplet.dstAttr.map { case (vid, distance) => (vid, triplet.attr + distance) }
          if (dstPlus != triplet.srcAttr) {
            triplet.sendToSrc(dstPlus)
          }
        },
        (a, b) => mergeMaps(a, b),
        TripletFields.Dst
      )

      g2 = g2.outerJoinVertices(newVertices)((_, oldAttr, opt) => mergeMaps(oldAttr, opt.get))

      n = g2.vertices.map { case (vid, srcAttr) => srcAttr.size }.reduce(_ + _)
      //println("number\t" + n)
    }

    g2
  }

  //最小生成树
  def prime[VD](g: Graph[VD, Double], origin: VertexId) = {
    //初始化，其中属性为（boolean, double，Long）类型，boolean用于标记是否访问过，double为加入当前顶点的代价，Long是上一个顶点的id
    var g2 = g.mapVertices((vid, _) => (false, if (vid == origin) 0 else Double.MaxValue, -1L))

    for (i <- 1L to g.vertices.count()) {
      //从没有访问过的顶点中找出 代价最小 的点
      val currentVertexId = g2.vertices.filter(!_._2._1).reduce((a, b) => if (a._2._2 < b._2._2) a else b)._1
      //更新currentVertexId邻接顶点的‘double’值
      val newDistances = g2.aggregateMessages[(Double, Long)](
        triplet => if (triplet.srcId == currentVertexId && !triplet.dstAttr._1) { //只给未确定的顶点发送消息
          triplet.sendToDst((triplet.attr, triplet.srcId))
        },
        (x, y) => if (x._1 < y._1) x else y,
        TripletFields.All
      )
      //newDistances.foreach(x => println("currentVertexId\t"+currentVertexId+"\t->\t"+x))
      //更新图形
      g2 = g2.outerJoinVertices(newDistances) {
        case (vid, vd, Some(newSum)) => (vd._1 || vid == currentVertexId, math.min(vd._2, newSum._1), if (vd._2 <= newSum._1) vd._3 else newSum._2)
        case (vid, vd, None) => (vd._1 || vid == currentVertexId, vd._2, vd._3)
      }
      //g2.vertices.foreach(x => println("currentVertexId\t"+currentVertexId+"\t->\t"+x))
    }

    //g2
    g.outerJoinVertices(g2.vertices)((vid, srcAttr, dist) => (srcAttr, dist.getOrElse(false, Double.MaxValue, -1)._2, dist.getOrElse(false, Double.MaxValue, -1)._3))
  }
}
