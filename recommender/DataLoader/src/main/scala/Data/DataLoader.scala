package Data

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


/*
* movies数据集
* movieid:电影ID
* title:电影题目
* genres:电影流派
* */

case class Movie(val movieId:Int,val title:String,val genres:String)

/*评分数据集（rating）
*userId:用户ID
* movieId:电影ID
*rating:评分等级（0.5-5）
* timestamp:时间戳
* */

case class Rating(val userId:Int,val movieId:Int,val rating:Double,val timestamp:Int)

/*标签数据集（tags）
userId：用户ID
movieId:电影ID
rating:标签内容
timestamp:时间戳
* */
case class Tag(val userId:Int,val movieId:Int,val tag:String,val timestamp:Int)

//mongodb连接配置
case class MongoConfig(val uri:String,val db:String)

//elasticsearch连接配置
case class ESconfig(val httpHosts:String,val transportHosts:String,val index:String,val clutername:String)

object DataLoader {
  val MOVIE_DATA_PATH ="F:\\ideaproject\\Movies\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH ="F:\\ideaproject\\Movies\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH ="F:\\ideaproject\\Movies\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION ="Ratings"
  val MONGODB_TAG_COLLECTION ="Tags"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

   System.setProperty("es.set.netty.runtime.available.processors", "false")
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.158.10:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.158.10:9200",
      "es.transportHosts" -> "192.168.158.10:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )
//sparkconf配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.core"))
    //sparksession
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    //加载数据集,RDD转DF
    import spark.implicits._


    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(item=>{
      val attr = item.split("\\,",-1)
      Movie(attr(0).toInt,attr(1).trim,attr(2).trim)
    }).toDF()


    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item=>{
      val attr = item.split("\\,")
      Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(item=>{
      val attr = item.split(",",-1)
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
    }).toDF()


    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    //将数据保存mongodb
//    storeDataInMongoDB(movieDF,ratingDF,tagDF)

    //将tags数据处理，处理后的形式mid |tag1|tag2|tag3,和movies融合
    import org.apache.spark.sql.functions._
    val newTag = tagDF.groupBy($"movieId")
      .agg(concat_ws("|",collect_set($"tag")).as("tags"))
      .select("movieId","tags")

    val movieWithTagsDF = movieDF.join(newTag,Seq("movieId","movieId"),"left")
    //声明ES配置的隐式参数
    implicit val esConfig = ESconfig(config("es.httpHosts"), config("es.transportHosts"),config("es.index"),config("es.cluster.name"))
    //将数据保存ES
    storeDataInES(movieWithTagsDF)

    spark.stop()
  }
  //将数据保存mongodb方法
  def storeDataInMongoDB(movieDF: DataFrame,ratingDF: DataFrame,tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
      //连接mongodb
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //删除原有的数据库
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
    //写入mongodb
    movieDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //数据建立索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("movieId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("userId" ->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("movieId" ->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("userId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("movieId" -> 1))

    mongoClient.close()

  }
  //将数据保存ES方法
  def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESconfig): Unit ={
   //配置
    val settings:Settings = Settings.builder().put("cluster.name",eSConfig.clutername).build()
    //新建ES客户端
    val esClient = new PreBuiltTransportClient(settings)
    //将transporthost添加到esClient
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    //清除原有数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))
    // 将数据写入ES
    movieDF
      .write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","movieId")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index+"/"+ES_MOVIE_INDEX)
  }
}
