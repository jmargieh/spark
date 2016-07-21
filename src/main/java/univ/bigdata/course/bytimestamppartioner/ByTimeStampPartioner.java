package univ.bigdata.course.bytimestamppartioner;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * This class make two partions of the dataset, one for training and the other for testing
 * partitioning is done by timestamp value.
 * testing dataset contains newest 30% movie reviews.
 * training dataset contains oldest 70% movie reviews.
 * @author Jawad
 *
 */
public class ByTimeStampPartioner {

	private static final String DEFAULT_DATA_FILE_PATH = "movies-simple4.txt";
	private static final String TRAINING_FILENAME = "movies-train.txt";
	private static final String TESTING_FILENAME = "movies-test.txt";
	private static final double TESTING_PARTITION = 0.3;
	private static final double TRAINING_PARTITION = 0.7;
	
    /** SparkContext object. */
    transient private JavaSparkContext sc;
	
	public static void main(String[] args) throws Exception {

		
	    JavaSparkContext sc;
		FSDataOutputStream testOutputWriter = null, trainOutputWriter = null;
        try {
        	
        	Configuration configuration = new Configuration();
        	FileSystem fs = FileSystem.get(configuration);
        	
        	Path testOutFile = new Path(TESTING_FILENAME);
        	testOutputWriter = fs.create(testOutFile);
        	
        	Path trainOutFile = new Path(TRAINING_FILENAME);
        	trainOutputWriter = fs.create(trainOutFile);
        	
        } catch (IOException e) {
            e.printStackTrace();
        }
        SparkConf conf = new SparkConf().setAppName("MovieApplication");
        conf.setMaster("local[2]");
        sc = new JavaSparkContext(conf);	
	 
        sc.clearCallSite();
        sc.clearJobGroup();
        JavaRDD < String > rddMovieData = sc.textFile(DEFAULT_DATA_FILE_PATH).cache();
        
        long count = rddMovieData.count();
        
        JavaPairRDD<String, String> timeStampedEntriesdRdd = rddMovieData.mapToPair(new PairFunction < String, String, String > () {
            public Tuple2 < String, String > call(String s) {
                String[] data = s.split("\t");
                String timestamp = data[5].split(":")[1].trim();
                return new Tuple2 < String, String > (timestamp, s);
            }
        });
        
        TimeStampComparator.order = "desc";
        List<Tuple2<String, String>> timeStampedEntriesdRddList = timeStampedEntriesdRdd.takeOrdered(Math.toIntExact(Math.round(count * TESTING_PARTITION)), TimeStampComparator.VALUE_COMP);
        
        for (int i = 0; i < timeStampedEntriesdRddList.size(); i++) {
        	//testOutputWriter.writeChars(timeStampedEntriesdRddList.get(i)._2 + "\n");
        	testOutputWriter.writeBytes(timeStampedEntriesdRddList.get(i)._2 + "\n");
        }
        
        TimeStampComparator.order = "asc";
        timeStampedEntriesdRddList = timeStampedEntriesdRdd.takeOrdered(Math.toIntExact(Math.round(count * TRAINING_PARTITION)), TimeStampComparator.VALUE_COMP);
        
        for (int i = 0; i < timeStampedEntriesdRddList.size(); i++) {
        	//trainOutputWriter.writeChars(timeStampedEntriesdRddList.get(i)._2 + "\n");
        	trainOutputWriter.writeBytes(timeStampedEntriesdRddList.get(i)._2 + "\n");
        }
        
        testOutputWriter.close();
        trainOutputWriter.close();
        sc.close();
        sc.stop();
        
	}
	
	
	/**
	 * Comperator timestamp
	 * @author Jawad
	 *
	 */
    static class TimeStampComparator implements Comparator < Tuple2 < String, String >> , Serializable {
        private static final long serialVersionUID = 1L;
        private static String order = "asc";
        private static final TimeStampComparator VALUE_COMP = new TimeStampComparator();

        @Override
        public int compare(Tuple2 < String, String > t1, Tuple2 < String, String > t2) {
        	int timestamp1 = Integer.parseInt(t1._1);
        	int timestamp2 = Integer.parseInt(t2._1);
        	
        	
        	if(timestamp1 > timestamp2) {
        		if(order.compareTo("desc") == 0) {
        			return -1;
        		}else {
        			return 1;
        		}
        	} else if (timestamp1 == timestamp2) {
        		return 0;
        	}
        	if(order.compareTo("desc") == 0) {
        		return 1;
        	} else {
        		return -1;
        	}
            //return t1._1.compareTo(t2._1);
        }
    }	
	
}
