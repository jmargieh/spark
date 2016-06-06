package univ.bigdata.course.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

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
  
  /** The outout file writer. */
  transient private FileWriter outputWriter;
  
  /** SparkContext object. */
  transient private JavaSparkContext sc;
  
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
      outputWriter.write("Getting list of total movies average.");
      outputWriter.write("\n\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
    SparkConf conf = new SparkConf().setAppName("Movie Application");
    conf.setMaster("local[1]");
    sc = new JavaSparkContext(conf);
  }

 
  /**
   * <br>For each entry retrieved from getTopKMoviesAverageNFW ( getTopKMoviesAverage - No File Write), writes in the output file. 
   * <br>
   * <br>@see getTopKMoviesAverageNFW(int topCount);
   * <br>@param topCount - number of top movies to return
   * <br>@return topMovieScoreMap   The list of movies where each @{@link Movie} includes it's average
   */
  public Map<String, Float> getTopKMoviesAverage(int topCount) {
    Map<String, Float> topMovieScoreMap = getTopKMoviesAverageNFW(topCount);
    
    try {
      outputWriter.write("TOP" + topCount + ".");
      outputWriter.write("\n");
      for(String key : topMovieScoreMap.keySet()) {
        outputWriter.write("Movie{productId='" + key + "', score=" + topMovieScoreMap.get(key) + "}");
        outputWriter.write("\n");
      }
      outputWriter.write("\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return topMovieScoreMap;

  }

  /**
   * <br>Extract movies, count and average against the movies using SparkContext but No File Write at this position. 
   * <br>To be used by multiple methods sharing common logic. 
   * <br>
   * <br>@see getTopKMoviesAverageNFW(int topCount);
   * <br>@param topCount - number of top movies to return
   * <br>@return topMovieScoreMap   The map of movies where each @{@link Movie} includes it's average
   */
  private Map<String, Float> getTopKMoviesAverageNFW(int topCount) {
    
    sc.clearCallSite();
    sc.clearJobGroup();
    JavaRDD<String> rddMovieData = sc.textFile(inputFileName).cache();

    JavaRDD<String> rddMovieScoreData = rddMovieData.map(new Function<String, String>() {

      @Override
      public String call(String arg0) throws Exception {
        String[] data = arg0.split("\t");
        String movieScore = data[4].split(":")[1].trim();
        String movieName = data[0].split(":")[1].trim();

        return movieName + "\t" + movieScore; 
      }
    });

    JavaPairRDD<String, MovieReview> rddPairScoreData = rddMovieScoreData.mapToPair(new PairFunction<String,String,MovieReview>() {
      public Tuple2<String,MovieReview> call(String s) {
         String[] data = s.split("\t");
         return new Tuple2<String,MovieReview>(data[0], new MovieReview(Float.parseFloat(data[1].trim()), 1));
      }
   });
    
    JavaPairRDD<String, MovieReview> movieScoreKeys = rddPairScoreData.reduceByKey(new Function2<MovieReview, MovieReview, MovieReview>() {
      
      @Override
      public MovieReview call(MovieReview i1, MovieReview i2) {
        i1.score += i2.score;
        i1.count += i2.count;
        return i1;
      }
   });

    List<Tuple2<String, MovieReview>> topScoreList = movieScoreKeys.mapToPair(new PairFunction<Tuple2<String,MovieReview>, String, MovieReview>() {

      @Override
      public Tuple2<String, MovieReview> call(Tuple2<String, MovieReview> t) throws Exception {
        t._2.score /= t._2.count;
        return t;
      }
    }).takeOrdered(topCount, ScoreComparator.VALUE_COMP);
    
    Map<String, Float> topMovieScoreMap = new LinkedHashMap<>();
    for(int i = 0; i < topScoreList.size(); i++) {
      Tuple2<String, MovieReview> topScore = topScoreList.get(i);
      topMovieScoreMap.put(topScore._1, Math.round ((topScore._2.score) * 100000) / 100000.0f);
    }

    return topMovieScoreMap;
  }
  
  /**
   * <br>Extract movies, count and average against the movies using SparkContext finally returns the total average. 
   * <br>
   * <br>@return movieRatingAvg   The total movie average score.
   */
  public float totalMoviesAverageScore() {
    
    sc.clearCallSite();
    sc.clearJobGroup();
    
    JavaRDD<String> rddMovieData = sc.textFile(inputFileName).cache();

    JavaRDD<Float> mapMovileScoreData = rddMovieData.map(new Function<String, Float>() {

      @Override
      public Float call(String arg0) throws Exception {
        String[] data = arg0.split("\t");
        float movieScore = 0;

        if( data.length >= 4) {
          String[] strArr = data[4].split(":");
          if(strArr.length > 1) {
            try { 
              movieScore = Float.parseFloat(strArr[1].trim());
            }
            catch(Exception e) {
              System.err.println("<<ERROR>> " + arg0);
              e.printStackTrace();
            }
          }
        }
        
        return movieScore; 
      }
    });
    
    Float totalMovieScore = mapMovileScoreData.reduce(new Function2<Float, Float, Float>() {

      @Override
      public Float call(Float arg0, Float arg1) throws Exception {
        return arg0 + arg1;
      }
    });
    
    int totalMovieCount = mapMovileScoreData.map(new Function<Float, Integer>() {

      @Override
      public Integer call(Float arg0) throws Exception {
        return 1;
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {

      @Override
      public Integer call(Integer arg0, Integer arg1) throws Exception {
        return arg0 + arg1;
      }
    });
    
    float movieRatingAvg = totalMovieScore / totalMovieCount;
    
    try {
      outputWriter.write("Total average: " + movieRatingAvg);
      outputWriter.write("\n\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return movieRatingAvg;
  }
  
  
  /**
   * <br>Closes all the open handlers before leaving.
   */
  public void closeHandlers() {
    try {
      if(outputWriter != null) {
        outputWriter.write("THE END.");
        outputWriter.write("\n\n");
        outputWriter.flush();
        outputWriter.close();
        sc.close();
      }
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
  static class ScoreComparator implements Comparator<Tuple2<String, MovieReview>>, Serializable {
    private static final long serialVersionUID = 1L;

    private static final ScoreComparator VALUE_COMP = new ScoreComparator();
    
    @Override
    public int compare(Tuple2<String, MovieReview> t1, Tuple2<String, MovieReview> t2) {
      return -t1._2.score.compareTo(t2._2.score); 
    }
  }
  
}
