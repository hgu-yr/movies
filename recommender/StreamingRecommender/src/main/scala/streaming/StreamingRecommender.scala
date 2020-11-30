package streaming



import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

case class MongoConfig(uri:String,db:String)

case class Recommendation(rid:Int,r:Double)
case class UserRecs(userId:Int,recs:Seq[Recommendation])
case class MovieRecs(movieId:Int,resc:Seq[Recommendation])

object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("master")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.158.10:27017/recommender"))
}
object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.158.10:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"

    )

    //sparkconf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.core"))
    //sparksession
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(2))

    //广播电影相似度矩阵
    val simMoviesMatrix = spark
      .read
      .option("uri",config("mongo.uri"))
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load
      .as[MovieRecs]
      .rdd
      .map{ item =>
        (item.movieId,item.resc.map(x=>(x.rid,x.r)).toMap)
      }.collectAsMap()

    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    val abc = sc.makeRDD(1 to 2)
    abc.map(x=> simMoviesMatrixBroadCast.value.get(1)).count()

    //创建kafka连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "192.168.158.10:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))

    val ratingStream = kafkaStream.map{case msg =>
    val attr = msg.value().split("\\|")
      (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }

    ratingStream.foreachRDD{ rdd=>
      rdd.map{case (userId,movieId,rating,timestamp)=>
      println(">>>>>>>>>>")

      //获取最近的m次电影评分
        val userRcentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,userId,ConnHelper.jedis)

        //获取电影p最相似的K个电影
        val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,movieId,userId,simMoviesMatrixBroadCast.value)

        //计算待选电影的推荐优先级
        val streamRecs = computeMoviesScores(simMoviesMatrixBroadCast.value,userRcentlyRatings,simMovies)

        //将数据保存到mongodb
        saveRecsToMongoDB(userId,streamRecs)

      }.count()

    }
    ssc.start()
    ssc.awaitTermination()
  }



  //获取当前最近电影的评分
  def getUserRecentlyRating(num:Int,userId:Int,jedis:Jedis):Array[(Int,Double)] ={
    jedis.lrange("userId:" + userId.toString,0,num).map{item =>
      val attr = item.split("\\|")
      (attr(0).trim.toInt,attr(1).trim.toDouble)
    }.toArray
  }

//获取与当前电影相似的电影

  def getTopSimMovies(num:Int,movieId:Int,userId:Int,simMovies:scala.collection.Map[Int,Map[Int,Double]])(implicit mongoConfig: MongoConfig): Array[Int] ={
    //获取当前电影的所有的相似电影
    val allSimMovies = simMovies.get(movieId).get.toArray

    //获取用户已经看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("userId" -> userId)).toArray.map{item =>
    item.get("movieId").toString.toInt
    }
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2>_._2).take(num).map(x=>x._1)
  }

  //计算待选电影的推荐分数
  def computeMoviesScores(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],userRecentlyRatings:Array[(Int,Double)],topSimMovies:Array[Int]):Array[(Int,Double)] ={

    //用于保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()

    //用于保存每一个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int,Int]()

    //用于保存每一个电影的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    for(topSimMovie <- topSimMovies;userRecentlyRating <- userRecentlyRatings){
      val simScore = getMoviesSimScore(simMovies,userRecentlyRating._1,topSimMovie)
      if(simScore > 0.6){
        score += ((topSimMovie,simScore * userRecentlyRating._2))
        if(userRecentlyRating._2 > 3){
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        }
        else {
          decreMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        }
      }
    }
    score.groupBy(_._1).map{case(movieId,sims) =>
      (movieId,sims.map(_._2).sum / sims.length + log(increMap(movieId)) - log(decreMap(movieId)))
    }.toArray
  }

  //获取两个电影之间的相似度
  def getMoviesSimScore(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],userRatingMovie:Int,topSimMovie:Int):Double ={
      simMovies.get(topSimMovie) match {
        case Some(sim) =>sim.get(userRatingMovie) match {
          case Some(score) => score
          case None => 0.0
      }
        case None => 0.0
      }
  }

  //取2的对数
  def log(m:Int):Double ={
    math.log(m) / math.log(2)
  }

  //将数据保存到mongodb
  def saveRecsToMongoDB(userId:Int,streamRecs:Array[(Int,Double)])(implicit  mongoConfig: MongoConfig): Unit ={
    //到streamRecs连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streamRecsCollection.insert(MongoDBObject("userId" -> userId,"recs" -> streamRecs.map(x=> x._1+":"+x._2).mkString("|")))
  }
}
