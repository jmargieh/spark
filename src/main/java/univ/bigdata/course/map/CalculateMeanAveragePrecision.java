package univ.bigdata.course.map;


import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class CalculateMeanAveragePrecision implements Serializable {
	
	/** Default serial version UID. */
    private static final long serialVersionUID = 1L;	
    
    /** The input train file */
    private String trainFile;
    
    /** Where to write the test file **/
    private String testFileName;
    
    /** SparkContext object. */
    transient private JavaSparkContext sc;
    
    
	public CalculateMeanAveragePrecision(String trainFile, String testFileName) {

        this.trainFile = trainFile;
        this.testFileName = testFileName;
        SparkConf conf = new SparkConf().setAppName("MapApplication");
        //conf.setMaster("local[2]");
        sc = new JavaSparkContext(conf);
	}
	
	
	public void calculateMAP() {
		
		this.sc.clearCallSite();
        this.sc.clearJobGroup();
        JavaRDD < String > movieTrainData = this.sc.textFile(this.trainFile);
        JavaRDD < String > movieTestData = this.sc.textFile(this.testFileName);
        
        // merge training and testing sets
        JavaRDD <String> allData = movieTestData.union(movieTrainData);
        
        // use zipwithIndex to generate a unique Long Id for userId after applying distinct
        JavaPairRDD<String, Long> userIdToInt = allData.map(new Function<String, String>() {

			@Override
			public String call(String s) throws Exception {
				String[] data = s.split("\t");
				String userId = data[1].split(":")[1].trim();
				return userId;
			}
		}).distinct().zipWithUniqueId();
        
     // use zipwithIndex to generate a unique Long Id for movieId after applying distinct
        JavaPairRDD<String, Long> movieIdToInt = allData.map(new Function<String, String>() {

			@Override
			public String call(String s) throws Exception {
				String[] data = s.split("\t");
				String movieId = data[0].split(":")[1].trim();
				return movieId;
			}
		}).distinct().zipWithUniqueId();

        
        final Map<String, Long> userIdToIntMap = userIdToInt.collectAsMap();
        final Map<String, Long> movieIdToIntMap = movieIdToInt.collectAsMap();
        
        // build Rating rdd from the training set
        JavaRDD<Rating> trainRatings = movieTrainData.map(
                new Function<String,Rating>() {
                	@Override
                    public Rating call(String s) throws Exception {
                        String[] data = s.split("\t");
                        String movieId = data[0].split(":")[1].trim();
                        String userId = data[1].split(":")[1].trim();
                        String movieScore = data[4].split(":")[1].trim();
                        
                        //Rating(user: Int, product(movieId): Int, rating: Double)
                        Rating rating = new Rating(userIdToIntMap.get(userId).intValue(), movieIdToIntMap.get(movieId).intValue(), Double.valueOf(movieScore));
                        return rating;
                    }
                }
        );
        
        // build Rating rdd from the testing set
        JavaRDD<Rating> testRatings = movieTestData.map(
                new Function<String,Rating>() {
                	@Override
                    public Rating call(String s) throws Exception {
                        String[] data = s.split("\t");
                        String movieId = data[0].split(":")[1].trim();
                        String userId = data[1].split(":")[1].trim();
                        String movieScore = data[4].split(":")[1].trim();
                        //Rating(user: Int, product(movieId): Int, rating: Double)
                        Rating rating = new Rating(userIdToIntMap.get(userId).intValue(), movieIdToIntMap.get(movieId).intValue(), Double.valueOf(movieScore));
                        return rating;
                    }
                }
        );
        
        
        
        // Build the recommendation model using ALS /// training the model ///
        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainRatings), rank, numIterations, 0.01);
        
        
        // predict using the model on the testing data
        JavaRDD<Tuple2<Object, Object>> userProducts = testRatings.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );
        
        // predict using the model
        // map will swap between key and value(rating) in order to sort by ratings
        // then swap again to normal
        JavaPairRDD<Integer, Tuple2<Integer, Double>> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Integer, Tuple2<Integer, Double>>>() {
                            public Tuple2<Integer, Tuple2<Integer, Double>> call(Rating r) {
                                return new Tuple2<Integer, Tuple2<Integer, Double>>(r.user(),new Tuple2<Integer, Double>(r.product(), r.rating()));
                                        
                            }
                        }
                ));
        

        // get actual ratings from testRatings
        JavaPairRDD<Integer, Tuple2<Integer, Double>> actual =
                JavaPairRDD.fromJavaRDD(testRatings.map(
                		new Function<Rating, Tuple2<Integer, Tuple2<Integer, Double>>>() {
                			public Tuple2<Integer, Tuple2<Integer, Double>> call(Rating r) {
                                return new Tuple2<Integer, Tuple2<Integer, Double>>(r.user(),new Tuple2<Integer, Double>(r.product(), r.rating()));
                                        
                            }
                        }
                ));
        
        
     /** PRINTING NUMBER OF DISTINCT USERS IN TRAINING AND TESTING SETS ALONG WITH RATINGS NUMBER **/
        
        // test ratings count
        long testRatingCount = testRatings.count();
        // distinct test users count
        long testUserCount = testRatings.map(
                new Function<Rating, Object>() {
                    public Object call(Rating r) throws Exception {
                        return r.user();
                    }
                }
        ).distinct().count();
        
        // train ratings count
        long trainRatingCount = trainRatings.count();
        // distinct train users count
        long trainUserCount = trainRatings.map(
                new Function<Rating, Object>() {
                    public Object call(Rating r) throws Exception {
                        return r.user();
                    }
                }
        ).distinct().count();
        
        
        /** PRINTING NUMBER OF DISTINCT USERS IN TRAINING AND TESTING SETS ALONG WITH RATINGS NUMBER **/
        
        JavaPairRDD<Integer, Iterable<Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>>>> actualAndPredictedSorted = actual.join(predictions).filter(new Function<Tuple2<Integer,Tuple2<Tuple2<Integer,Double>,Tuple2<Integer,Double>>>, Boolean>() {
			
			@Override
			public Boolean call(
					Tuple2<Integer, Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>>> v1) {
					if(v1._2._2._1.toString().compareTo(v1._2._1._1.toString()) == 0 ) {
						return true;
					}
					return false;
			}
		}).sortByKey().groupByKey();
        
        // userId with his Ratings Sorted by predicted rating
        JavaPairRDD<Integer, CustomRating[]> usersAndSortedRatingsPairRdd = actualAndPredictedSorted.mapToPair(new PairFunction<Tuple2<Integer,Iterable<Tuple2<Tuple2<Integer,Double>,Tuple2<Integer,Double>>>>, Integer, CustomRating[]>() {
			
        	@Override
			public Tuple2<Integer, CustomRating[]> call(
					Tuple2<Integer, Iterable<Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>>> > t)
					throws Exception {
				int i = 0;
				CustomRating[] customRatings = new CustomRating[Iterables.size(t._2)];
				java.util.Iterator<Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>>> iter = t._2.iterator();
				while(iter.hasNext()){
					Tuple2<Tuple2<Integer, Double>, Tuple2<Integer, Double>> res = iter.next();
					customRatings[i] = new CustomRating(res._2._2, res._1._2);
					i++;
				}
				Arrays.sort(customRatings,CustomRating.RateComparator);
				 return new Tuple2 < Integer, CustomRating[]> (t._1,customRatings);
			}
        });
        
        // userId along with his map value
        JavaPairRDD<Integer, Double> usersAndMaps = usersAndSortedRatingsPairRdd.mapToPair(new PairFunction<Tuple2<Integer,CustomRating[]>, Integer, Double>() {

			@Override
			public Tuple2<Integer, Double> call(
					Tuple2<Integer, CustomRating[]> t) throws Exception {
					int hitCounter = 0;
					double sum = 0;
					for (int i = 0 ; i< t._2.length; i++) {
						if (t._2[i].actual >= 3) {
							hitCounter++;
							double res = (hitCounter/(i+1));
							sum = sum + res;
						}
					}
					if (hitCounter == 0) {
						sum = 0;
					} else {
					sum = sum/hitCounter;
					}
					
				return new Tuple2 < Integer, Double > (t._1,sum);
			}
        	
		});
        
        
        JavaRDD<Double> onlyMaps = usersAndMaps.map(new Function<Tuple2<Integer,Double>, Double>() {

			@Override
			public Double call(Tuple2<Integer, Double> v1) throws Exception {
				return v1._2;
			}
        	
		});
        
        if(onlyMaps.count() == 0) {
        	System.err.print("=====================\n");
        	System.err.print("ERROR : Something went wrong!! please provide valid testing and training sets!!\n");
        	System.err.print("=====================\n");
        	sc.close();
        	System.exit(-1);
        }
        
        // total user maps count
        double mapsTotal = onlyMaps.reduce(new Function2<Double, Double, Double>() {
			
			@Override
			public Double call(Double v1, Double v2) throws Exception {
				return v1+ v2;
			}
		});
        
        double map = mapsTotal/onlyMaps.count();
        System.out.print("=====================\n");
        System.out.print("Got " + testRatingCount + " test ratings from "
                + testUserCount + " users and " + trainRatingCount + " train ratings from " + trainUserCount + " .\n");
        System.out.print("=====================\n");
        System.out.print("Mean Average Precision is : " + map + "\n");        
        System.out.print("=====================\n");
        
        sc.close();
        sc.stop();
        
	}

	// custom Rating comperator
	static class CustomRating implements Serializable, Comparable<CustomRating> {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		double actual;
        double predicted;
    	CustomRating(double predicted, double actual) {
            this.actual = actual;
            this.predicted = predicted;
        }
		@Override
		public int compareTo(CustomRating o) {
			return (int) (this.actual - o.actual);
		}
		
		public static Comparator<CustomRating> RateComparator 
        = new Comparator<CustomRating>() {

			public int compare(CustomRating o1, CustomRating o2) {
				if(o1.predicted > o2.predicted) {
					return -1;
				} else if (o1.predicted < o2.predicted) {
					return 1;
				}
				return 0;
}

};

		
    }
	
}
