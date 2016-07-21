package univ.bigdata.course.pagerank;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class PageRank implements Serializable {

    /** Default serial version UID. */
    private static final long serialVersionUID = 1L;

    /** The input filename */
    private String inputFileName;

    /** SparkContext object. */
    transient private JavaSparkContext sc;
    
    public PageRank(String inputFileName) {
        this.inputFileName = inputFileName;
        SparkConf conf = new SparkConf().setAppName("PageRankApplication");
        //conf.setMaster("local[2]");
        sc = new JavaSparkContext(conf);
    }
	
    public void calculatePageRank() {
        sc.clearCallSite();
        sc.clearJobGroup();

        JavaRDD < String > rddFileData = sc.textFile(inputFileName).cache();
        
        JavaRDD < String > rddMovieData = rddFileData.map(new Function < String, String > () {

            @Override
            public String call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                String movieId = data[0].split(":")[1].trim();
                String userId = data[1].split(":")[1].trim();
                return movieId + "\t" + userId;
            }
        });

        JavaPairRDD<String, String> rddPairReviewData = rddMovieData.mapToPair(new PairFunction < String, String, String > () {

            @Override
            public Tuple2 < String, String > call(String arg0) throws Exception {
                String[] data = arg0.split("\t");
                return new Tuple2 < String, String > (data[0], data[1]);
            }
        }).distinct();
        
        JavaPairRDD<String, Tuple2<String, String>> carte = rddPairReviewData.join(rddPairReviewData);
        
        carte = carte.filter(s->(!s._2._1.equals(s._2._2)));
        
        JavaPairRDD <String,String> graph = carte.mapToPair(users -> new Tuple2<String,String>(users._2._1,users._2._2));


    	JavaRDD<String> userIdPairsString = graph.map(new Function < Tuple2<String, String>, String > () {

          	//Tuple2<Tuple2<MovieId, userId>, Tuple2<movieId, userId>>
              @Override
              public String call (Tuple2<String, String> t) throws Exception {
              	return t._1 + " " + t._2;
              }
          });

        try {
			JavaPageRank.calculatePageRank(userIdPairsString, 100);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        sc.close();
        sc.stop();
        
    }
	
}
