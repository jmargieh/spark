package univ.bigdata.course.recommend;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class Recommend implements Serializable {

	/** Default serial version UID. */
    private static final long serialVersionUID = 1L;
    
    /** The input filename */
    private String inputFile;
    
    
    /** SparkContext object. */
    transient private JavaSparkContext sc;
    
    private ArrayList<String> userList = new ArrayList<String>();
    
    //transient private FSDataOutputStream recOutputWriter;
    /** The outout file writer. */
    //transient private FileWriter OutputWriter;
    
    //transient private FileSystem fs;

	private String outputFileName;
    
	public Recommend(String inputFile, String outputFileName, ArrayList<String> userList) {
        this.inputFile = inputFile;
        this.userList = userList;
        this.outputFileName = outputFileName;
        SparkConf conf = new SparkConf().setAppName("RecommendApplication");
        //conf.setMaster("local[2]");
        sc = new JavaSparkContext(conf);
	}
	
	public void Recommendation() throws IOException, URISyntaxException {
		System.setProperty("HADOOP_USER_NAME", "vagrant");
		FileWriter OutputWriter = null;
		try {
			OutputWriter = new FileWriter(outputFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
		
		this.sc.clearCallSite();
        this.sc.clearJobGroup();
        JavaRDD < String > movieData = this.sc.textFile(this.inputFile).cache();
        
        // use zipwithIndex to generate a unique Long Id for userId after applying distinct
        JavaPairRDD<String, Long> userIdToInt = movieData.map(new Function<String, String>() {

			@Override
			public String call(String s) throws Exception {
				String[] data = s.split("\t");
				String userId = data[1].split(":")[1].trim();
				return userId;
			}
		}).distinct().zipWithUniqueId();
        
        
        // use zipwithIndex to generate a unique Long Id for movieId after applying distinct
        JavaPairRDD<String, Long> movieIdToInt = movieData.map(new Function<String, String>() {

			@Override
			public String call(String s) throws Exception {
				String[] data = s.split("\t");
				String movieId = data[0].split(":")[1].trim();
				return movieId;
			}
		}).distinct().zipWithUniqueId();
        
        // need to reverse from Long Id to String Id.
        JavaPairRDD<Long,String> movieIntToId = movieIdToInt.mapToPair(new PairFunction<Tuple2<String,Long>, Long, String>() {

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Long> t) {
					return new Tuple2<Long, String>(t._2, t._1);
			}
        	
		});
        
        Map<String, Long> userIdToIntMap = userIdToInt.collectAsMap();
        Map<String, Long> movieIdToIntMap = movieIdToInt.collectAsMap();
        Map<Long, String> movieIntToIdMap = movieIntToId.collectAsMap();
        // build the Rating Rdd
        JavaRDD<Rating> ratings = movieData.map(
                new Function<String,Rating>() {
                	@Override
                    public Rating call(String s) throws Exception {
                        String[] data = s.split("\t");
                        String movieId = data[0].split(":")[1].trim();
                        String userId = data[1].split(":")[1].trim();
                        String movieScore = data[4].split(":")[1].trim();
                        
                        //Rating(user: Int, product(movieId): Int, rating: Double)
                        Rating rating = new Rating(userIdToIntMap.get(userId).intValue(), movieIdToIntMap.get(movieId).intValue(), Double.parseDouble(movieScore));
                        return rating;
                    }
                }
        );
        
     // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);
        
        for(int j=0; j<this.userList.size(); j++) {
            if (userIdToIntMap.get(this.userList.get(j)) != null) { 
    	        int userIdConverted = userIdToIntMap.get(this.userList.get(j)).intValue();
    	        Rating[] userRating =  model.recommendProducts(userIdConverted, 10);
    	        OutputWriter.write("Recommendations for "+ this.userList.get(j) +":\n");
    	        //recOutputWriter.writeBytes("Recommendations for "+ this.userList.get(j) +":\n");
    	        for(int i=0; i< userRating.length; i++){
    	        	OutputWriter.write((i+1) + ". " + movieIntToIdMap.get(userRating[i].product()) + "\n");
    	        	//recOutputWriter.writeBytes(i + ". " + movieIntToIdMap.get(userRating[i].product()) + "\n");
    	        }
    	        OutputWriter.write("======================================\n");
    	        //recOutputWriter.writeBytes("======================================\n");
            } else {
            	OutputWriter.write(this.userList.get(j) + " NOT FOUND!!\n");
            	//recOutputWriter.writeBytes(this.userList.get(j) + " NOT FOUND!!\n");
            	OutputWriter.write("======================================\n");
            	//recOutputWriter.writeBytes("======================================\n");
            }
        }
        
        
        
//        if (recOutputWriter != null) {
//        	recOutputWriter.close();
//        	recOutputWriter.flush();
//        }
//        fs.close();
        if (OutputWriter != null) {
        	OutputWriter.flush();
        	OutputWriter.close();
        }
        sc.close();
        sc.stop();
	}	
}
