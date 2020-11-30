package off

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
//基于评分数据的隐语义模型离线推荐

case class MovieRating(val userId:Int,val movieId:Int,val rating:Double,val timestamp:Int)

case class Movie(val movieId:Int,val title:String,val genres:String)

case class MongoConfig(val uri:String,val db:String)

//定义基于预测评分的用户推荐列表
case class Recommendation(rid:Int,r:Double)
case class UserRecs(userId:Int,resc:Seq[Recommendation])

//基于隐语义模型电影相似度特征向量的列表
case class MovieRecs(movieId:Int,resc:Seq[Recommendation])

object OffLineRcommender {
  //定义表名
  val MONGODB_RATING_COLLECTION ="Ratings"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.158.10:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //sparkconf配置
    val sparkConf = new SparkConf().setAppName("OffLineRecommender").setMaster(config("spark.core"))
      .set("spark.executor.memory","128G").set("spark.driver.memory","64G").set("spark.local.dir","D:\\offdata")

    val spark =  SparkSession.builder().config(sparkConf).config("spark.sql.broadcastTimeout", "36000").getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //加载数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.userId,rating.movieId,rating.rating)) //去掉时间戳
      .cache()

//取数，去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.movieId.toInt)
      .cache()


    //训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))
    val(rank,iterations,lambda) = (50,5,0.01)
    val model = ALS.train(trainData,rank,iterations,lambda)

    //基于用户和用户的隐特征，计算预测评分，得到用户推荐列表
    //计算user,movies的笛卡尔积,空的评分矩阵
    val userMovies = userRDD.cartesian(movieRDD)

    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating >0)
        .map(rating => (rating.user,(rating.product,rating.rating)))
        .groupByKey()
        .map{
          case(userId,recs) => UserRecs(userId,recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x =>Recommendation(x._1,x._2)))
        }
        .toDF()
    userRecs.write
        .option("uri",mongoConfig.uri)
        .option("collection",USER_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    //基于电影隐特征计相似度矩阵，得到电影的相似度列表

    val movieFeatures = model.productFeatures.map{
      case(movieId,features) => (movieId,new DoubleMatrix(features))
    }

    val movieRecs = movieFeatures.cartesian(movieFeatures)
        .filter{
          case(a,b) => a._1 != b._1  //过滤掉自己跟自己的笛卡尔积
        }
        .map{
          case(a,b) => {
            val simScore = this.consinSim(a._2,b._2)
            (a._1,(b._1,simScore))
          }
        }
        .filter(_._2._2 > 0.6) //过滤出相似度大于0.6
        .groupByKey()
        .map{
          case(movieId,items) => MovieRecs(movieId,items.toList.map(x=> Recommendation(x._1,x._2)))
        }
        .toDF()

    movieRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    spark.stop()
  }

//求向量余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}
