class Model{

}

case class WordCount(word:String, lang:String, count:Int)

case class Person(name:String,age:Int,phone:String)

/**
  * 通讯录分词表
  * @param uid uid
  * @param country
  * @param province
  * @param city
  * @param word 分词
  * @param source 分词类型
  * @param source_id 来源uid
  * @param synonym 同义词
  */
case class Word(uid:String,country:String,province:String,city:String,word:String,source:String,source_id:String,synonym:String)

/**
  * 聊天记录分词表
  * @param uid
  * @param msg_id
  * @param word
  * @param source
  * @param time
  * @param platform
  * @param synonym
  */
case class MsgWord(uid:String,msg_id:String,word:String,source:String,time:Long,platform:String,synonym:String)
case class RealName(uid:String,name:String)
case class Group(id1:Long,id2:Long,uid:String)
case class RelationShip(u_uid:String,c_uid:String,ship:String,ship_type:String)