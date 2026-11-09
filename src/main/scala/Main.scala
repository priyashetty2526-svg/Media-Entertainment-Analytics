
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object Main {

  // Function to save Spark DataFrame as a CSV file
  def saveCsv(df: DataFrame, filePath: String): Unit = {

    val path = Paths.get(filePath)
    Files.createDirectories(path.getParent)

    val writer = Files.newBufferedWriter(
      path,
      StandardCharsets.UTF_8
    )

    def escapeCsv(value: String): String =
      "\"" + value.replace("\"", "\"\"") + "\""

    try {
      writer.write(df.columns.map(escapeCsv).mkString(","))
      writer.newLine()

      df.collect().foreach { row =>
        val values = row.toSeq.map { value =>
          escapeCsv(if (value == null) "" else value.toString)
        }

        writer.write(values.mkString(","))
        writer.newLine()
      }
    } finally {
      writer.close()
    }

    println("Saved: " + filePath)
  }

  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Media Entertainment Data Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("\n======================================")
    println("MEDIA AND ENTERTAINMENT DATA ANALYTICS")
    println("======================================")

    // 1. LOAD DATASET
    val movies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dataset/movies.csv")

    val ratings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dataset/ratings.csv")

    // 2. DISPLAY MOVIE DATA
    println("\nMOVIE DATA")
    movies.show(10, false)

    // 3. DISPLAY RATING DATA
    println("\nRATING DATA")
    ratings.show(10)

    // 4. DATA CLEANING
    val cleanMovies = movies
      .dropDuplicates("movieId")
      .na.drop()

    val cleanRatings = ratings
      .dropDuplicates("userId", "movieId")
      .na.drop()

    println("\nDATA CLEANING COMPLETED")

    // 5. JOIN MOVIES AND RATINGS
    val movieRatings = cleanMovies.join(
      cleanRatings,
      Seq("movieId"),
      "inner"
    )

    println("\nCOMBINED MOVIE AND RATING DATA")
    movieRatings.show(10, false)

    // 6. GENRE ANALYSIS
    val genreAnalysis = movieRatings
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre")
      .agg(
        count("*").alias("total_ratings"),
        round(avg("rating"), 2).alias("average_rating")
      )
      .orderBy(desc("total_ratings"))

    println("\n1. GENRE ANALYSIS")
    genreAnalysis.show(30, false)

    saveCsv(genreAnalysis, "output/genre_analysis.csv")

    // 7. TOP RATED MOVIES
    val topMovies = movieRatings
      .groupBy("movieId", "title")
      .agg(
        count("rating").alias("rating_count"),
        round(avg("rating"), 2).alias("average_rating")
      )
      .filter(col("rating_count") >= 20)
      .orderBy(desc("average_rating"), desc("rating_count"))
      .limit(10)

    println("\n2. TOP 10 MOVIES")
    topMovies.show(10, false)

    saveCsv(topMovies, "output/top_movies.csv")

    // 8. YEARLY MOVIE RELEASE TRENDS
    val yearlyTrends = cleanMovies
      .withColumn(
        "release_year",
        regexp_extract(col("title"), "\\((\\d{4})\\)", 1)
      )
      .filter(col("release_year") =!= "")
      .groupBy("release_year")
      .count()
      .withColumnRenamed("count", "total_movies")
      .orderBy("release_year")

    println("\n3. YEARLY MOVIE RELEASE TRENDS")
    yearlyTrends.show(50)

    saveCsv(yearlyTrends, "output/yearly_trends.csv")

    // 9. USER ACTIVITY ANALYSIS
    val userActivity = cleanRatings
      .groupBy("userId")
      .agg(
        count("*").alias("total_ratings"),
        round(avg("rating"), 2).alias("average_user_rating")
      )
      .orderBy(desc("total_ratings"))

    println("\n4. TOP 10 ACTIVE USERS")
    userActivity.show(10)

    saveCsv(userActivity, "output/user_activity.csv")

    // 10. RATING TRENDS OVER TIME
    val ratingTrends = cleanRatings
      .withColumn(
        "rating_date",
        to_date(from_unixtime(col("timestamp")))
      )
      .groupBy("rating_date")
      .agg(
        count("*").alias("total_ratings"),
        round(avg("rating"), 2).alias("average_rating")
      )
      .orderBy("rating_date")

    println("\n5. RATING TRENDS")
    ratingTrends.show(20)

    saveCsv(ratingTrends, "output/rating_trends.csv")

    // 11. DATASET SUMMARY
    println("\n6. DATASET SUMMARY")

    println("Total Movies: " + cleanMovies.count())

    println("Total Ratings: " + cleanRatings.count())

    println(
      "Total Users: " +
        cleanRatings.select("userId").distinct().count()
    )

    // 12. PROJECT COMPLETION
    println("\n======================================")
    println("PROJECT EXECUTION COMPLETED!")
    println("Check the output folder for CSV files.")
    println("======================================")

    spark.stop()
  }
}
