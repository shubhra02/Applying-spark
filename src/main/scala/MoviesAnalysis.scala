import org.apache.spark.rdd.RDD

class MoviesAnalysis {

  def getRatingData(ratingInitialRDD: RDD[String]) = {
    ratingInitialRDD.map { record =>
      val array = record.split("::")
      val movieId = array(1).toInt
      val rating = array(2).toInt
      (movieId, rating)
    }
  }

  def getMovieData(movieInitialRDD: RDD[String]): RDD[(Int, String)] = {
    movieInitialRDD.map { record =>
      val array = record.split("::")
      val movieId = array(0).toInt
      val movieName = array(1)
      (movieId, movieName)

    }
  }

  def getJoinData(ratingData: RDD[(Int, Int)], movieData: RDD[(Int, String)]): RDD[(String, Int)] = {
    ratingData.join(movieData).map(pairData => (pairData._2._2, pairData._2._1))
  }

  /**
    * Method to get the top 10 ratings of movies
    * @param movieInitialRDD - RDD of type String to extract movie data
    * @param ratingInitialRDD - RDD of type String to extract rating data
    * @return - Array[(String, Int)] of 10 elements containing movie name and average rating of corresponding movie
    */

  def getTopRatingInMovies(movieInitialRDD: RDD[String], ratingInitialRDD: RDD[String]): Array[(String, Int)] = {
    val ratingData = getRatingData(ratingInitialRDD)
    val movieData = getMovieData(movieInitialRDD)
    val joinData= getJoinData(ratingData, movieData)
    val groupedData = joinData.groupByKey().map(pairData => (pairData._1, (pairData._2.sum / pairData._2.size)))
    groupedData.sortBy(-_._2).take(10)
  }

  /**
    * Method to get the top 10 rated movies
    * @param movieInitialRDD- RDD of type String to extract movie data
    * @param ratingInitialRDD- RDD of type String to extract rating data
    * @return - Array[(String, Int)] of 10 elements containing movie name and total ratings of corresponding movie
    */
  def getTopRatedMovies(movieInitialRDD: RDD[String], ratingInitialRDD: RDD[String]): Array[(String, Int)] = {
    val movieData = getMovieData(movieInitialRDD)
    val ratingData = getRatingData(ratingInitialRDD)
    val joinData = getJoinData(ratingData, movieData)
    val groupedData = joinData.groupByKey().map(pairData => (pairData._1, pairData._2.size))
    groupedData.sortBy(-_._2).take(10)
  }

  def getRating(ratingRDD: RDD[String]): RDD[(Int, String)] = {
    ratingRDD.map { record =>
      val array = record.split("::")
      val userId = array(0)
      val movieId = array(1).toInt
      (movieId, userId)
    }
  }

  /**
    * Method to get the movie with highest ratings given by user
    * @param ratingInitialRDD- RDD of type String to extract rating data
    * @return - tuple(String, Int) containing the userId and the movieId of the movie with maximum ratings
    */
  def getMostRatingByUser(ratingInitialRDD: RDD[String], movieInitialRDD: RDD[String]): (Int, Int) = {
    val ratingData: RDD[(Int, String)] = getRating(ratingInitialRDD)
    val moviesData: RDD[(Int, String)] = getMovieData(movieInitialRDD)
    val joinData = ratingData.join(moviesData)
    ratingData.groupByKey().mapValues(_.size).sortBy(-_._2).take(1).headOption.fold((0, 0))(identity)
  }


  def getMovieGenre(movieRDD: RDD[String]): RDD[(Int, String)] = {
    movieRDD.filter { record =>
      val genre = record.split("::")(2)
      genre.contains("Action")
    }.map { record =>
      val array = record.split("::")
      val movieId = array(0).toInt
      val genre = array(2)
      (movieId, genre)
    }
  }

  /**
    * Method to get the total ratings of movies with genre 'Action'
    * @param ratingInitialRDD- RDD of type String to extract rating data
    * @param movieInitialRDD- RDD of type String to extract movie data
    * @return - A Long number specifying the number of ratings of 'Action' movies
    */
  def getActionMovieRatings(ratingInitialRDD: RDD[String], movieInitialRDD: RDD[String]): Long = {
    val ratingData: RDD[(Int, Int)] = getRatingData(ratingInitialRDD)
    val moviesData: RDD[(Int, String)] = getMovieGenre(movieInitialRDD)
    ratingData.join(moviesData).count

  }

  def getProgrammers(userRDD: RDD[String]): RDD[(Int, String)] = {
    userRDD.filter{record =>
      val occupation = record.split("::")(3)
      occupation == "12"
    }.map{record =>
      val array = record.split("::")
      val userId = array(0).toInt
      val occupation = array(3)
      (userId, occupation)
    }
  }


  def getRatingDataByProgrammers(ratingRDD: RDD[String]): RDD[(Int, Int)] = {
    ratingRDD.map { record =>
      val array = record.split("::")
      val userId = array(0).toInt
      val movieId = array(1).toInt
      (userId, movieId)
    }
  }

  /**
    * Method to get the top 10 movies rated by the Programmers
    * @param userInitialRDD- RDD of type String to extract user data
    * @param ratingInitialRDD- RDD of type String to extract rating data
    * @return - Array[(Int, Int)] of 10 elements containing movieId and toital ratings of that movie
    */
  def getRatingsByProgrammer(userInitialRDD: RDD[String], ratingInitialRDD: RDD[String]): Array[(Int, Int)] = {
    val userData = getProgrammers(userInitialRDD)
    val ratingData = getRatingDataByProgrammers(ratingInitialRDD)
    val joinData = userData.join(ratingData).map(pairData => (pairData._2._2, pairData._2._2))
    joinData.groupByKey().map(x => (x._1, x._2.size)).sortBy(-_._2).take(10)
  }
}