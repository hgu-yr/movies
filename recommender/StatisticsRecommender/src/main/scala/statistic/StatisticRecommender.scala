package statistic

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(val movieId:Int,val title:String,val genres:String)

case class Rating(val userId:Int,val movieId:Int,val rating:Double,val timestamp:Int)

case class MongoConfig(val uri:String,val db:String)

//电影类别top10推荐对象
case class Recommendation(rId:Int,r:Double) //rid:movieId,r:rating
case class GenresRcommendation(genres:String,resc:Seq[Recommendation])

object StatisticRecommender {
  //定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION ="Ratings"
  //统计表名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.158.10:27017/recommender",
      "mongo.db" -> "recommender"

    )
    //sparkconf配置
    val sparkConf = new SparkConf().setAppName("SatisticRecommender").setMaster(config("spark.core"))
    //sparksession
    val spark =  SparkSession.builder().config(sparkConf).getOrCreate()
    //加载数据集,RDD转DF
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //从mongodb加载数据
    val ratingDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()
//创建ratings临时表
    ratingDF.createOrReplaceTempView("ratings")
    //不同的统计结果
    //1.历史热门电影统计，历史评分数据最多(mid,count)
    val rateMoreMoviesDF = spark.sql("select movieId,count(movieId) as count " +
      "from ratings " +
      "group by movieId " +
      "order by count desc")
    //结果写入mongodb
    soreDFInMongoDB(rateMoreMoviesDF,RATE_MORE_MOVIES)

    //2.近期热门统计，按照年月筛选最近的评分数据，统计评分个数
    //创建日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册udf，把时间戳换成年月格式
    spark.udf.register("changeData",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //对原始数据做预处理，去掉uerId
    val ratingOfYearMonth = spark.sql("select movieId,rating,changeData(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyMoviesDF = spark.sql("select movieId,count(movieId) as count,yearmonth " +
      "from ratingOfMonth " +
      "group by yearmonth,movieId " +
      "order by yearmonth desc,count desc")
    soreDFInMongoDB(rateMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)

    //3.优质电影推荐，统计电影的平均评分
    val averageMoviesDF = spark.sql("select movieId,title,avg(rating) as avg from ratings  order by avg desc")
    soreDFInMongoDB(averageMoviesDF,AVERAGE_MOVIES)

    //4.各类别电影top统计
    //定义出所有类别
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family",
    "Fantasy","Foreign","History","Horror","Music","Mystery","Romance","Science","Tv","Thriller","War","Western")

    //把平均评分加入movies表
    val movieWithScore = movieDF.join(averageMoviesDF,"movieId")
    val genresRDD = spark.sparkContext.makeRDD(genres)
    //对电影类别做笛卡尔积
    val genresTopMovieDF = genresRDD.cartesian(movieWithScore.rdd)
        .filter {
          case (genres,movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
        }
        .map{
          case(genres,movieRow) => (genres,(movieRow.getAs[Int]("movieId"),movieRow.getAs[Double]("avg")))
        }
        .groupByKey()
        .map{
          case(genres,items) => GenresRcommendation(genres,items.toList.sortWith(_._2>_._2).take(10).map(item=>Recommendation(item._1,item._2)))
        }
        .toDF()

    soreDFInMongoDB(genresTopMovieDF,GENRES_TOP_MOVIES)
    spark.stop()

  }
   def soreDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig:MongoConfig): Unit ={
     df.write
       .option("uri",mongoConfig.uri)
       .option("collection",collection_name)
       .mode("overwrite")
       .format("com.mongodb.spark.sql")
       .save()
   }
}
