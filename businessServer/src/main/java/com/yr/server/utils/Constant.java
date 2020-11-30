package com.yr.server.utils;
//定义整个业务系统的常量
public class Constant {

    //mongodb中的表名
    public static final String MONGO_DATABASE ="recommender";

    public static final String MONGO_MOVIE_COLLECTION ="Movie";

    public static final String MONGO_RATING_COLLECTION ="Ratings";

    public static final String MONGO_TAG_COLLECTION = "Tags";

    public static final String MONGO_USER_COLLECTION = "User";

    public static final String MONGO_AVERAGE_MOVIES = "AverageMovies";

    public static final String MONGO_GENRES_TOP_MOVIES = "GenresTopMovies";

    public static final String MONGO_RATE_MORE_MOVIES = "RateMoreMovies";

    public static final String MONGO_RATE_MORE_RECENTLY_MOVIES = "RateMoreMovies";

    public static final String MONGO_USER_RECS_COLLECTION = "UserRecs";

    public static final String MONGO_MOVIE_RECS_COLLECTION = "MovieRecs";

    public static final String MONGO_STREAM_RECS_COLLECTION = "StreamRecs";

    //ES
    public static final String ES_INDEX = "recommender";

    public static final String ES_TYPE = "Movie";

    //redis
    public static final int USER_RATING_QUEUE_SIZE = 20;

    public static final String USER_RATING_LOG_PREFIX = "USER_RATING_LOG_PREFIX";


}
