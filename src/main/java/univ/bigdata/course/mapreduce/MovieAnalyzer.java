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

public class MovieAnalyzer implements Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private String inputFileName;
  transient private FileWriter outputWriter;
  transient private JavaSparkContext sc;
  
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

}
