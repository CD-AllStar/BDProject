import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkOperateHBase {
  def main(args: Array[String]) {

    val conf = HBaseConfiguration.create()
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ShortestPath")
        sparkConf.setMaster("spark://hadoop005:7077")
        sparkConf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar"))
        sparkConf.set("spark.executor.cores", "2")
        sparkConf.set("spark.executor.memory", "4g")
    val sc = new SparkContext(sparkConf)
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "student")
    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()

    //遍历输出
    stuRDD.map({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      ("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
    }).collect().foreach(println)
  }
}