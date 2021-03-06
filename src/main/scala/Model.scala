import org.apache.spark.graphx.VertexId

class Model{

}


case class MsgWord(uid:String,msg_id:String,word:String,source:String,time:Long,platform:String,synonym:String)
case class Group(id1:Long,id2:Long,uid:String)
case class CommonFriends(u_uid:String,c_uid:String,count:Int)
case class PageRank(vertexId: Long,score:Double)
case class SplitedName(uid:String,name:String,source_id:String,word:String,group:String,synonym:String)
case class CityWord(uid:String,country:String,province:String,city:String,word:String,source:String,source_id:String,synonym:String)