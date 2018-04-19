import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.setAppName("Streaming-Test")
    conf.setMaster("spark://hadoop005:7077")
    conf.setJars(Array[String]("out/artifacts/SparkClient_jar/SparkClient.jar"))
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "8g")

    val ssc = new StreamingContext(conf, Seconds(10))


  }
}
