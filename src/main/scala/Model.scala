import org.apache.spark.graphx.VertexId

class Model{

}

case class WordCount(word:String, lang:String, count:Int)

case class Person(name:String,age:Int,phone:String)

case class CityWord(uid:String,country:String,province:String,city:String,word:String,source:String,source_id:String,synonym:String)
case class CityMedia(uid:String,country:String,province:String,city:String,word:String,source:String,source_id:String,synonym:String)
case class MsgWord(uid:String,msg_id:String,word:String,source:String,time:Long,platform:String,synonym:String)
case class RealName(uid:String,name:String)
case class Group(id1:Long,id2:Long,uid:String)
case class RelationShip(u_uid:String,c_uid:String,ship:String,ship_type:String,synonym:String)
case class CommonFriends(u_uid:String,c_uid:String,count:Int)
case class PageRank(vertexId: Long,score:Double)