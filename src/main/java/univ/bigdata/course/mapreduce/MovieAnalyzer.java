package univ.bigdata.course.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import univ.bigdata.course.mapreduce.MovieAnalyzer.MovieReview;

/**
 * <br>This class performs assigned tasks with the help of JavaRDD in SparkContext.
 * <br>It receives the the input and out file name in its constructor and keeps globally.
 * <br>Each tasks via a method is called with parameters uses this input file to load/reload the data and writes results into output file. 
 */
public class MovieAnalyzer implements Serializable {

    /** Default serial version UID. */
    private static final long serialVersionUID = 1L;

    /** The input filename */
    private String inputFileName;

//    /** The outout file writer. */
    transient private FileWriter outputWriter;
    
    /** The outout file writer. */
    //transient private FSDataOutputStream outputWriter;

    /** SparkContext object. */
    transient private JavaSparkContext sc;

    //transient private FileSystem fs;
    
    private JavaRDD < String > rddMovieData;
    
    
    /**
     * <br>MovieAnalyzer constructor with input and output files.
     * <br>
     * <br>@param inputFileName   The input file provided.
     * <br>@param outputFileName  The output file where results will be written.
     */
    public MovieAnalyzer(String inputFileName, String outputFileName) {
        this.inputFileName = inputFileName;
        try {
        	outputWriter = new FileWriter(outputFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SparkConf conf = new SparkConf().setAppName("MovieApplication");
        //conf.setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        
    	rddMovieData = sc.textFile(inputFileName).cache(); 
    }

    /**
     * <br>Returns the movie name which has highest average.
     * <br>
     * <br>@return highestAverageMovie  The movie name and it average entry having higest average.
     */
    public void movieWithHighestAverage() {
        Entry < String, Float > highestAverageMovie = getTopKMoviesAverageNFW(1).entrySet().iterator().next();

        try {
            outputWriter.write("movieWithHighestAverage");
            outputWriter.write("\n");
            outputWriter.write("The movie with highest average:  " + "Movie{productId='" + highestAverageMovie.getKey() + "', score=" + highestAverageMovie.getValue() + "}");
            outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <br>Returns the map of top reviewed words and with the word count.
     * <br>
     * <br>@param topWordCount  The top count of the words.
     * <br>@return topReviewWordsMap  The map containing to ${topWordCount} words and it count.
     */
    public void moviesReviewWordsCount(int topWordCount) {
        Map < String, Long > topReviewWordsMap = topYMoviesReviewTopXWordsCountNFR(moviesCountHelper(), topWordCount);

        try {
            outputWriter.write("moviesReviewWordsCount " + topWordCount);
            outputWriter.write("\n");
            for (String key: topReviewWordsMap.keySet()) {
                outputWriter.write("Word = [" + key + "], number of occurrences [" + topReviewWordsMap.get(key) + "].");
                outputWriter.write("\n");
            }
            //outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * <br>Extract movies, count and comments against the movies using SparkContext.
     * <br>Sort them by reviews count, then loop over the topMovies list sorted by reviews count and make a new map of words for these reviews
     * <br>Sort the list of type wordsCount map entries by count value, lexicographically will be preserved.
     * <br>
     * <br>@param topMovieCount The number of top review movies.
     * <br>@param topWordCount  The  number of top words to return.
     * <br>@return topMovieWordMap  A map of words to count, ordered by count in decreasing order.
     */
    public void topYMoviesReviewTopXWordsCount(int topMovieCount, int topWordCount) {

        Map < String, Long > topMovieWordMap = topYMoviesReviewTopXWordsCountNFR(topMovieCount, topWordCount);

        try {
            outputWriter.write("topYMoviesReviewTopXWordsCount " + topMovieCount + " " + topWordCount);
            outputWriter.write("\n");
            for (String key: topMovieWordMap.keySet()) {
                outputWriter.write("Word = [" + key + "], number of occurrences [" + topMovieWordMap.get(key) + "].");
                outputWriter.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * <br>For each entry retrieved from getTopKMoviesAverageNFW ( getTopKMoviesAverage - No File Write), writes in the output file. 
     * <br>
     * <br>@see getTopKMoviesAverageNFW(int topCount);
     * <br>@param topCount - number of top movies to return
     * <br>@return topMovieScoreMap   The list of movies where each @{@link Movie} includes it's average
     */
    public void getTopKMoviesAverage(int topCount) {
        Map < String, Float > topMovieScoreMap = getTopKMoviesAverageNFW(topCount);

        try {
            outputWriter.write("getTopKMoviesAverage " + topCount);
            outputWriter.write("\n");
            for (String key: topMovieScoreMap.keySet()) {
                outputWriter.write("Movie{productId='" + key + "', score=" + topMovieScoreMap.get(key) + "}");
                outputWriter.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * <br>Extract movies, count and average against the movies using SparkContext but No File Write at this position. 
     * <br>To be used by multiple methods sharing common logic. 
     * <br>
     * <br>@see getTopKMoviesAverageNFW(int topCount);
     * <br>@param topCount - number of top movies to return
     * <br>@return topMovieScoreMap   The map of movies where each @{@link Movie} includes it's average
     */
    private Map < String, Float > getTopKMoviesAverageNFW(int topCount) {

        //sc.clearCallSite();
        //sc.clearJobGroup();
        //JavaRDD < String > rddMovieData = sc.textFile(inputFileName).cache();

        JavaRDD < String > rddMovieScoreData = rddMovieData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String movieScore = data[4].split(":")[1].trim();
                String movieName = data[0].split(":")[1].trim();

                return movieName + "\t" + movieScore;
            }
        });

        JavaPairRDD < String, MovieReview > rddPairScoreData = rddMovieScoreData.mapToPair(new PairFunction < String, String, MovieReview > () {
            public Tuple2 < String, MovieReview > call(String s) {
                String[] data = s.split("\t");
                return new Tuple2 < String, MovieReview > (data[0], new MovieReview(Float.parseFloat(data[1].trim()), 1));
            }
        });

        JavaPairRDD < String, MovieReview > movieScoreKeys = rddPairScoreData.reduceByKey(new Function2 < MovieReview, MovieReview, MovieReview > () {

            @Override
            public MovieReview call(MovieReview i1, MovieReview i2) {
                i1.score += i2.score;
                i1.count += i2.count;
                return i1;
            }
        });

        List < Tuple2 < String, MovieReview >> topScoreList = movieScoreKeys.mapToPair(new PairFunction < Tuple2 < String, MovieReview > , String, MovieReview > () {

            @Override
            public Tuple2 < String, MovieReview > call(Tuple2 < String, MovieReview > t) throws Exception {
                t._2.score /= t._2.count;
                return t;
            }
        }).takeOrdered(topCount, ScoreComparator.VALUE_COMP);

        Map < String, Float > topMovieScoreMap = new LinkedHashMap < > ();
        for (int i = 0; i < topScoreList.size(); i++) {
            Tuple2 < String, MovieReview > topScore = topScoreList.get(i);
            topMovieScoreMap.put(topScore._1, Math.round((topScore._2.score) * 100000) / 100000.0f);
        }

        return topMovieScoreMap;
    }

    /**
     * <br>Extract movies, count and average against the movies using SparkContext finally returns the total average.
     * Filters the result against product/movie provided. 
     * <br>
     * <br>@return movieRatingAvg   The movie average score.
     */
    public void totalMovieAverage(String productId) {

        //sc.clearCallSite();
        //sc.clearJobGroup();

        //JavaRDD < String > rddMovieData = sc.textFile(inputFileName).cache();

        JavaPairRDD < String, Float > mapMovileScoreData = rddMovieData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String movie = data[0].split(":")[1].trim();
                String score = data[4].split(":")[1].trim();

                return movie + "\t" + score;
            }
        }).mapToPair(new PairFunction < String, String, Float > () {

            @Override
            public Tuple2 < String, Float > call(String t) throws Exception {
                String[] data = t.split("\t");
                String movie = data[0].trim();
                String score = data[1].trim();
                return new Tuple2 < String, Float > (movie, Float.parseFloat(score));
            }
        }).filter(new Function < Tuple2 < String, Float > , Boolean > () {

            @Override
            public Boolean call(Tuple2 < String, Float > v) throws Exception {
                return v._1.equalsIgnoreCase(productId);
            }
        });

        long totalMovieCount = mapMovileScoreData.count();

        float totalMovieScore = mapMovileScoreData.reduceByKey(new Function2 < Float, Float, Float > () {

            @Override
            public Float call(Float v1, Float v2) throws Exception {
                return v1 + v2;
            }
        }).collectAsMap().get(productId);

        float movieRatingAvg = Math.round((totalMovieScore / totalMovieCount) * 100000.0) / 100000.0f;
        try {
        	outputWriter.write("totalMovieAverage " + productId + "\n");
            outputWriter.write("Movies " + productId + " average is " + movieRatingAvg);
            outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <br>Extract movies, count and average against the movies using SparkContext finally returns the total average. 
     * <br>
     * <br>@return movieRatingAvg   The total movie average score.
     */
    public void totalMoviesAverageScore() {

        //sc.clearCallSite();
        //sc.clearJobGroup();

        //JavaRDD < String > rddMovieData = sc.textFile(inputFileName).cache();

        JavaRDD < Float > mapMovileScoreData = rddMovieData.map(new Function < String, Float > () {

            @Override
            public Float call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                float movieScore = 0;

                if (data.length >= 4) {
                    String[] strArr = data[4].split(":");
                    if (strArr.length > 1) {
                        try {
                            movieScore = Float.parseFloat(strArr[1].trim());
                        } catch (Exception e) {
                            System.err.println("<<ERROR>> " + arg0);
                            e.printStackTrace();
                        }
                    }
                }

                return movieScore;
            }
        });

        Float totalMovieScore = mapMovileScoreData.reduce(new Function2 < Float, Float, Float > () {

            @Override
            public Float call(Float arg0, Float arg1) throws Exception {
                return arg0 + arg1;
            }
        });

        int totalMovieCount = mapMovileScoreData.map(new Function < Float, Integer > () {

            @Override
            public Integer call(Float arg0) throws Exception {
                return 1;
            }
        }).reduce(new Function2 < Integer, Integer, Integer > () {

            @Override
            public Integer call(Integer arg0, Integer arg1) throws Exception {
                return arg0 + arg1;
            }
        });

        float movieRatingAvg = totalMovieScore / totalMovieCount;
        
        movieRatingAvg = (float) roundDouble(movieRatingAvg);
        

        try {
            outputWriter.write("totalMoviesAverageScore");
            outputWriter.write("\n");
            outputWriter.write("Total average: " + movieRatingAvg);
            outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	private double roundDouble(double number) {
		return Math.round (number * 100000.0) / 100000.0;
	}
    
    /**
     * <br>Extract movies, count, reviewers and reviews against the movies using SparkContext and retrieves the movie that reviewed by K users. 
     * <br>
     * <br>@param topCount - The number of top count users.
     * <br>@return The most popular movie reviewed. 
     */
    public void mostPopularMovieReviewedByKUsers(int topCount) {
    	    	
        //sc.clearCallSite();
        //sc.clearJobGroup();

        //JavaRDD < String > rddFileData = sc.textFile(inputFileName).cache();

        JavaRDD < String > rddMovieDataa = rddMovieData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String movieName = data[0].split(":")[1].trim();
                String movieScore = data[4].split(":")[1].trim();

                return movieName + "\t" + movieScore; //reviewUser; 
            }
        });

        JavaPairRDD < String, MovieReview > rddPairReviewData = rddMovieDataa.mapToPair(new PairFunction < String, String, MovieReview > () {

            @Override
            public Tuple2 < String, MovieReview > call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                return new Tuple2 < String, MovieReview > (data[0], new MovieReview(Float.parseFloat(data[1]), 1));
            }
        });

        JavaPairRDD < String, MovieReview > movieReviewedReducedByKey = rddPairReviewData.reduceByKey(new Function2 < MovieReview, MovieReview, MovieReview > () {

            @Override
            public MovieReview call(MovieReview arg0, MovieReview arg1) throws Exception {
                arg0.score += arg1.score;
                arg0.count += arg1.count;

                return arg0;
            }
        });

        JavaPairRDD < String, MovieReview > filteredMovieReviewed = movieReviewedReducedByKey.filter(new Function < Tuple2 < String, MovieReview > , Boolean > () {

            @Override
            public Boolean call(Tuple2 < String, MovieReview > v) throws Exception {
                return v._2.count >= topCount;
            }
        });

        List < Tuple2 < String, MovieReview >> mostReviewedTupleList = filteredMovieReviewed.mapToPair(new PairFunction < Tuple2 < String, MovieReview > , String, MovieReview > () {

            @Override
            public Tuple2 < String, MovieReview > call(Tuple2 < String, MovieReview > t) throws Exception {
                t._2.score /= t._2.count;
                return t;
            }
        }).takeOrdered(topCount, ScoreComparator.VALUE_COMP);

        try {
            outputWriter.write("mostPopularMovieReviewedByKUsers " + topCount);
            outputWriter.write("\n");
            outputWriter.write("Most popular movie with highest average score, reviewed by at least " + topCount + " users ");
            if(mostReviewedTupleList.size() !=0) {
            	outputWriter.write(mostReviewedTupleList.get(0)._1 +"\n");
            } else {
            	outputWriter.write("\n");
            }
            //outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <br>Extract movie, reviewer and its helpful values against a movie review using SparkContext. 
     * <br>And calculates the top K helpful using using the values of their helpfulness.
     * <br>
     * <br>@param topCount - number of top movies to return
     * <br>@return sortedHelpfulUserMap   The map of top K helpful users with their helpful value.
     */
    public void topKHelpfullUsers(int topCount) {

        //sc.clearCallSite();
        //sc.clearJobGroup();

        //JavaRDD < String > rddFileData = sc.textFile(inputFileName).cache();

        JavaRDD < String > rddMovieDataa = rddMovieData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String userName = data[1].split(":")[1].trim();
                String reviewHelpful = data[3].split(":")[1].trim();

                return userName + "\t" + reviewHelpful;
            }
        });

        JavaPairRDD < String, String > rddPairHelpfulData = rddMovieDataa.mapToPair(new PairFunction < String, String, String > () {

            @Override
            public Tuple2 < String, String > call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                return new Tuple2 < String, String > (data[0], data[1]);
            }
        });

        JavaPairRDD < String, String > movieHelpfulReducedByKey = rddPairHelpfulData.reduceByKey(new Function2 < String, String, String > () {

            @Override
            public String call(String arg0, String arg1) throws Exception {

                double a = Double.parseDouble(arg0.split("/")[0].trim()) + Double.parseDouble(arg1.split("/")[0].trim());
                double b = Double.parseDouble(arg0.split("/")[1].trim()) + Double.parseDouble(arg1.split("/")[1].trim());

                return a + "/" + b;
            }
        }).filter(new Function < Tuple2 < String, String > , Boolean > () {

            @Override
            public Boolean call(Tuple2 < String, String > v) throws Exception {
                return Double.parseDouble(v._2.split("/")[1].split("/")[0].trim()) != 0;
            }
        });


        JavaPairRDD < String, Double > rddmovieHelpfulMap = movieHelpfulReducedByKey.mapToPair(new PairFunction < Tuple2 < String, String > , String, Double > () {

            @Override
            public Tuple2 < String, Double > call(Tuple2 < String, String > t) throws Exception {

                double a = Double.parseDouble(t._2.split("/")[0].trim());
                double b = Double.parseDouble(t._2.split("/")[1].trim());

                return new Tuple2 < String, Double > (t._1, Math.round((a / b) * 100000.0) / 100000.0);
            }
        });

        int rddCount = (int)rddmovieHelpfulMap.count();
        
        int newTopCount = rddCount < topCount ? rddCount : topCount;
        
        List < Tuple2 < String, Double >> sortedHelpfulList = rddmovieHelpfulMap.takeOrdered(newTopCount, MapDoubleValueComparator.VALUE_COMP);
        Map < String, Double > sortedHelpfulUserMap = new TreeMap < > ();

        try {
            outputWriter.write("topKHelpfullUsers " + topCount);
            outputWriter.write("\n");
            for (Tuple2 < String, Double > tuple: sortedHelpfulList) {
                sortedHelpfulUserMap.put(tuple._1, tuple._2);
                outputWriter.write("User id = [" + tuple._1 + "], helpfulness [" + tuple._2 + "].");
                outputWriter.write("\n");
            }
            //outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(">>>Number of result is :: " + sortedHelpfulUserMap.size());

    }

    /**
     * <br>Extract movies/products, and their review scores in descending order and returned first product/movie.
     * <br>
     * <br>@return The most reviewed Product.
     */
    public void mostReviewedProduct() {
        Tuple2 < String, Long > mostReviewedProductTuple = reviewedProductEntryList().get(0);
        try {
            outputWriter.write("mostReviewedProduct");
            outputWriter.write("\n");
            outputWriter.write("The most reviewed movie product id is " + mostReviewedProductTuple._1);
            outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <br>Extract movie, reviewer and its review scores against a product/movie review using SparkContext. 
     * <br>and forms a list of product/movie with its review count in descending order.
     * <br>
     * <br>@return movieReviewedReducedTupleList   A list of product/movie with its review count in descending order.
     */
    public List < Tuple2 < String, Long >> reviewedProductEntryList() {

        //sc.clearCallSite();
        //sc.clearJobGroup();

        //JavaRDD < String > rddFileData = sc.textFile(inputFileName).cache();

        JavaRDD < String > rddMovieDataa = rddMovieData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String movieName = data[0].split(":")[1].trim();

                return movieName + "\t" + 1;
            }
        });

        JavaPairRDD < String, Long > rddPairReviewData = rddMovieDataa.mapToPair(new PairFunction < String, String, Long > () {

            @Override
            public Tuple2 < String, Long > call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                return new Tuple2 < String, Long > (data[0], Long.parseLong(data[1]));
            }
        });

        JavaPairRDD < String, Long > movieReviewedReducedByKey = rddPairReviewData.reduceByKey(new Function2 < Long, Long, Long > () {

            @Override
            public Long call(Long arg0, Long arg1) throws Exception {
                return arg0 + arg1;
            }
        });
        List < Tuple2 < String, Long >> movieReviewedReducedTupleList = movieReviewedReducedByKey.takeOrdered((int) movieReviewedReducedByKey.count(), MapLongValueComparator.VALUE_COMP);

        return movieReviewedReducedTupleList;
    }

    /**
     * <br>Extract top ${topCount} movie, reviewer and its review count against a product/movie review using SparkContext. 
     * <br>and forms a map of K entries of product/movie with its review count in descending order.
     * <br>
     * <br>@param topCount  Top count for review.
     * <br>@return reviewCountMap   A map top K product/movie with its review count in descending order.
     */
    public void reviewCountPerMovieTopKMovies(int topCount) {
        List < Tuple2 < String, Long >> reviewedProductEntryList = reviewedProductEntryList();
        int newTopCount = reviewedProductEntryList.size() < topCount ? reviewedProductEntryList.size() : topCount;
        List < Tuple2 < String, Long >> reviewCountEntryList = reviewedProductEntryList().subList(0, newTopCount);
        Map < String, Long > reviewCountMap = new LinkedHashMap < > ();
        try {
            outputWriter.write("reviewCountPerMovieTopKMovies " + topCount);
            outputWriter.write("\n");
            for (int i = 0; i < reviewCountEntryList.size(); i++) {
                Tuple2 < String, Long > tuple = reviewCountEntryList.get(i);
                reviewCountMap.put(tuple._1, tuple._2);
                outputWriter.write("Movie product id = [" + tuple._1 + "], reviews count [" + tuple._2 + "].");
                outputWriter.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <br>Extract total movie count that are being used for review.
     * <br>
     * <br>@return movieCount   Total number of movies being dealt with.
     */
    public int moviesCountHelper() {
        return reviewedProductEntryList().size();
    }

    /**
     * <br>Extract total movie count that are being used for review.
     * <br>
     * <br>@return movieCount   Total number of movies being dealt with.
     */
    public void moviesCount() {
        int movieCount = reviewedProductEntryList().size();

        //Total number of distinct movies reviewed [4].
        try {
            outputWriter.write("moviesCount");
            outputWriter.write("\n");
            outputWriter.write("Total number of distinct movies reviewed [" + movieCount + "].");
            outputWriter.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    

    /**
     * <br>Extract movies, count and comments against the movies using SparkContext but No File Write.
     * <br>Sort them by reviews count, then loop over the topMovies list sorted by reviews count and make a new map of words for these reviews
     * <br>Sort the list of type wordsCount map entries by count value, lexicographically will be preserved.
     * <br>
     * <br>@param topMovies The number of top review movies.
     * <br>@param topWords  The  number of top words to return.
     * <br>@return wordCountLinkedHashMap  A map of words to count, ordered by count in decreasing order.
     */
    private Map < String, Long > topYMoviesReviewTopXWordsCountNFR(int topMovies, int topWords) {
        if (topMovies < 0 || topWords < 0) {
            throw new IllegalArgumentException("Argument should be greater than zero");
        }

        JavaRDD < String > rddMovieDataa = rddMovieData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String movieScore = data[4].split(":")[1].trim();
                String movieName = data[0].split(":")[1].trim();
                //String movieComment = data[7].split(":")[1].trim();
                String movieComment = data[7].substring(data[7].indexOf(':') + 1).trim();

                return movieName + "\t\t" + movieScore + "\t\t" + movieComment;
            }
        });

        JavaPairRDD < String, MovieReview > rddPairReviewData = rddMovieDataa.mapToPair(new PairFunction < String, String, MovieReview > () {

            @Override
            public Tuple2 < String, MovieReview > call(String arg0) throws Exception {
                List < String > reviewList = new ArrayList < > ();
                String[] data = arg0.split("\t\t");
                reviewList.add(data[2]);
                return new Tuple2 < String, MovieReview > (data[0], new MovieReview(0f, 1, data[2]));
            }
        });

        JavaPairRDD < String, MovieReview > rddMovieReviewReducedByKey = rddPairReviewData.reduceByKey(new Function2 < MovieReview, MovieReview, MovieReview > () {

            @Override
            public MovieReview call(MovieReview arg0, MovieReview arg1) throws Exception {
                arg0.count += arg1.count;
                arg0.reviewComments = arg0.reviewComments.trim() + " " + arg1.reviewComments.trim();
                return arg0;
            }
        }).cache();

        int newTopMovies = (int)(rddPairReviewData.count() > topMovies ? topMovies : rddPairReviewData.count());
        
        
        
        
        JavaPairRDD <Tuple2<String,Long>, Long> sortedRdd = rddMovieReviewReducedByKey.mapToPair(new PairFunction < Tuple2 < String, MovieReview > , Tuple2<String,Long>, Long > () {

            @Override
            public Tuple2 < Tuple2<String,Long>, Long > call(Tuple2 < String, MovieReview > t) throws Exception {
                return new Tuple2 < Tuple2<String,Long>, Long > (new Tuple2<String,Long>(t._1,t._2.count), t._2.count);
            }
        }).sortByKey(new TupleMapLongComparator(), true, 100);
        
        
        JavaPairRDD <String,Long> sortedRddToPairs = sortedRdd.mapToPair(new PairFunction<Tuple2<Tuple2<String,Long>,Long>, String, Long>() {

			@Override
			public Tuple2<String, Long> call(
					Tuple2<Tuple2<String, Long>, Long> t) throws Exception {
				return new Tuple2 < String, Long > (t._1._1, t._1._2);
			}
        	
		});
        
        List < Tuple2 < String, Long >> rddMovieMostReviewed = 	sortedRddToPairs.take(newTopMovies);
        		
        List < String > mostReviewedMovieList = new ArrayList < > ();
        for (int i = 0; i < rddMovieMostReviewed.size(); i++) {
            mostReviewedMovieList.add(rddMovieMostReviewed.get(i)._1);
        }

        JavaPairRDD < String, MovieReview > movieReviewReducedByKey = rddMovieReviewReducedByKey.filter(new Function < Tuple2 < String, MovieReview > , Boolean > () {

            @Override
            public Boolean call(Tuple2 < String, MovieReview > v) throws Exception {
                return mostReviewedMovieList.contains(v._1);
            }
        });

        
        rddMovieReviewReducedByKey.unpersist();
        
        JavaPairRDD < String, Long > rddReducedReviewMap =
            movieReviewReducedByKey.map(new Function < Tuple2 < String, MovieReview > , MovieReview > () {

                @Override
                public MovieReview call(Tuple2 < String, MovieReview > v) throws Exception {
                    return v._2;
                }
            }).flatMap(new FlatMapFunction < MovieReview, MovieReview > () {
                public Iterable < MovieReview > call(MovieReview v) {
                    String comments = v.reviewComments;
                    List < MovieReview > movieReviews = new ArrayList < > ();

                    String[] words = comments.split(" ");
                    for (int k = 0; k < words.length; k++) {
                        movieReviews.add(new MovieReview(0f, 1, words[k].trim()));
                    }
                    return movieReviews;
                }
            }).mapToPair(new PairFunction < MovieReview, String, MovieReview > () {

                @Override
                public Tuple2 < String, MovieReview > call(MovieReview t) throws Exception {
                    return new Tuple2 < String, MovieReview > (t.reviewComments, t);
                }
            }).reduceByKey(new Function2 < MovieAnalyzer.MovieReview, MovieAnalyzer.MovieReview, MovieAnalyzer.MovieReview > () {

                @Override
                public MovieReview call(MovieReview v1, MovieReview v2) throws Exception {
                    v1.count += v2.count;
                    return v1;
                }
            }).mapToPair(new PairFunction < Tuple2 < String, MovieReview > , String, Long > () {

                @Override
                public Tuple2 < String, Long > call(Tuple2 < String, MovieReview > t) throws Exception {
                    return new Tuple2 < String, Long > (t._1, t._2.count);
                }
            }).cache();
        
        
        JavaPairRDD <Tuple2<String,Long>, Long> rddReducedReviewMapSorted = rddReducedReviewMap.mapToPair(new PairFunction < Tuple2 < String, Long > , Tuple2<String,Long>, Long > () {

            @Override
            public Tuple2 < Tuple2<String,Long>, Long > call(Tuple2 < String, Long > t) throws Exception {
                return new Tuple2 < Tuple2<String,Long>, Long > (new Tuple2<String,Long>(t._1,t._2), t._2);
            }
        }).sortByKey(new TupleMapLongComparator(), true, 100);
        
        JavaPairRDD <String,Long> rddReducedReviewMapSortedToPairs = rddReducedReviewMapSorted.mapToPair(new PairFunction<Tuple2<Tuple2<String,Long>,Long>, String, Long>() {

			@Override
			public Tuple2<String, Long> call(
					Tuple2<Tuple2<String, Long>, Long> t) throws Exception {
				return new Tuple2 < String, Long > (t._1._1, t._1._2);
			}
        	
		});
        
        
        int newTopWords = (int)(topWords > rddReducedReviewMap.count() ? rddReducedReviewMap.count() : topWords);
        
        List < Tuple2 < String, Long >> tupleReviewList = rddReducedReviewMapSortedToPairs.take(newTopWords);


        Map < String, Long > wordCountLinkedHashMap = new LinkedHashMap < > ();
        for (int i = 0; i < tupleReviewList.size(); i++) {
            Tuple2 < String, Long > tupleReview = tupleReviewList.get(i);
            wordCountLinkedHashMap.put(tupleReview._1, tupleReview._2);
        }

        return wordCountLinkedHashMap;
    }
    
    /**
     * NEW
     * @author Jawad
     *
     */
    private class TupleMapLongComparator implements Comparator<Tuple2<String,Long>>, Serializable {
        @Override
        public int compare(Tuple2<String,Long> tuple1, Tuple2<String,Long> tuple2) {
        	
            if (tuple1._2.compareTo(tuple2._2) == 0) {
                return tuple1._1.compareTo(tuple2._1);
            }
            return -tuple1._2.compareTo(tuple2._2);
        }
    }

    /**
     * <br>Closes all the open handlers before leaving.
     */
    public void closeHandlers() {
        try {
            if (outputWriter != null) {
                outputWriter.flush();
                outputWriter.close();
            }
            //fs.close();
            sc.close();
            sc.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * <br> Class holds the Movie information like score count and it review comments.
     */
    static class MovieReview implements Serializable {
        private static final long serialVersionUID = 1L;

        Float score = 0.0f;
        Long count = 1L;
        String reviewComments;
        MovieReview(float score, long count) {
            this.score = score;
            this.count = count;
            this.reviewComments = "";
        }
        MovieReview(float score, long count, String comments) {
            this.score = score;
            this.count = count;
            this.reviewComments = comments;
        }
        @Override
        public String toString() {
            return "MovieReview[Score=" + score + ", Count=" + count + ", Comments=" + reviewComments + "]";
        }
    }

    /**
     * <br> A comparator class to compare scores
     */
    static class ScoreComparator implements Comparator < Tuple2 < String, MovieReview >> , Serializable {
        private static final long serialVersionUID = 1L;

        private static final ScoreComparator VALUE_COMP = new ScoreComparator();

        @Override
        public int compare(Tuple2 < String, MovieReview > t1, Tuple2 < String, MovieReview > t2) {
        	if(t1._2.score.compareTo(t2._2.score) == 0) {
        		return t1._1.compareTo(t2._1);
        	}
            return -t1._2.score.compareTo(t2._2.score);
        }
    }

    /**
     * <br> A comparator class to compare Long values in a Map.
     */
    static class MapLongValueComparator implements Comparator < Tuple2 < String, Long >> , Serializable {
        private static final long serialVersionUID = 1L;

        private static final MapLongValueComparator VALUE_COMP = new MapLongValueComparator();

        @Override
        public int compare(Tuple2 < String, Long > o1, Tuple2 < String, Long > o2) {
            if (o1._2.compareTo(o2._2) == 0) {
                return o1._1.compareTo(o2._1);
            }
            return -o1._2.compareTo(o2._2);
        }
    }

    /**
     * <br> A comparator class to compare Double values in a Map.
     */
    static class MapDoubleValueComparator implements Comparator < Tuple2 < String, Double >> , Serializable {
        private static final long serialVersionUID = 1L;

        private static final MapDoubleValueComparator VALUE_COMP = new MapDoubleValueComparator();

        @Override
        public int compare(Tuple2 < String, Double > o1, Tuple2 < String, Double > o2) {
            if (o1._2.compareTo(o2._2) == 0) {
                return o1._1.compareTo(o2._1);
            }
            return -o1._2.compareTo(o2._2);
        }
    }
}