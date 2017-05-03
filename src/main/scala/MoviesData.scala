import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlobalObject {
  val conf = new SparkConf().setAppName("Spark Config").setMaster("local[*]").set("spark.executor.memory", "1g")
  val sparkContext = new SparkContext(conf)
  val movieFile = "src/main/resources/movies.txt"
  val ratingFile = "src/main/resources/ratings.txt"
  val userFile = "src/main/resources/users.txt"
}

object MoviesData extends App {
  import GlobalObject._;
  val moviesRDD = sparkContext.textFile(movieFile)
  val ratingsRDD = sparkContext.textFile(ratingFile)
  val usersRDD = sparkContext.textFile(userFile)
  val movieObject = new MoviesAnalysis

  val topRating = movieObject.getTopRatingInMovies(moviesRDD, ratingsRDD)
  println("The top 10 rating of movies are : ")
  topRating.foreach(println(_))

  val topRated = movieObject.getTopRatedMovies(moviesRDD, ratingsRDD)
  println("The top 10 rated movies are : ")
  topRated.foreach(println(_))

  val mostRating = movieObject.getMostRatingByUser(ratingsRDD, moviesRDD)
  println(s"The most ratings given by user is : $mostRating")

  val totalActionRatings = movieObject.getActionMovieRatings(ratingsRDD, moviesRDD)
  println(s"The total rating of movies with genre 'Action' are : $totalActionRatings")

  val ratingsByProgrammer = movieObject.getRatingsByProgrammer(usersRDD, ratingsRDD)
  println("The top 10 movies rated by the Programmers are : ")
  ratingsByProgrammer.foreach(println(_))


}