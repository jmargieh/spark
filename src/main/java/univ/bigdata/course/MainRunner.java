package univ.bigdata.course;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import univ.bigdata.course.map.CalculateMeanAveragePrecision;
import univ.bigdata.course.mapreduce.MovieAnalyzer;
import univ.bigdata.course.pagerank.PageRank;
import univ.bigdata.course.recommend.Recommend;


/**
 * This invoker class of SparkContext. <br>
 * It holds the main method to run the application. <br>
 * main() handles parameters like commands|recommend|map|pagerank and the action flow. <br>
 */
public class MainRunner {

  private static final String DEFAULT_COMMAND_FILE = "commands.txt";

  /**
   * The main method to invoke the application.
   * 
   * @param args    The command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
	
	 // check if no arguments are provided!
    if(args.length == 0) {
      System.err.println("Usage: ./bin/spark−submit −−class −−master yarn −−deploy−mode cluster −−executor−memory 1G −−num−executors 4 /path/to/final−project−1.0−SNAPSHOT.jar commands|recommend|map|pagerank FileName [MapFileName]");
      System.exit(-1);
    }
    
    String action = args[0];
    //map command
    if("map".equalsIgnoreCase(action)) {
    	if (args.length != 3) {
    		System.err.println("map command usage is : final−project −1.0−SNAPSHOT.jar map movies-train.txt movies-test.txt");
    		System.exit(-1);
    	} else {
        	CalculateMeanAveragePrecision map = new CalculateMeanAveragePrecision(args[1], args[2]);
        	map.calculateMAP();
    	}
    }
    
    // PageRank command
    if("pagerank".equalsIgnoreCase(action)) {
    	if (args.length != 2) {
    		System.err.println("pagerank command usage is : final−project −1.0−SNAPSHOT.jar pagerank fileName.txt");
    		System.exit(-1);
    	} else {
        	PageRank pageRank = new PageRank(args[1]);
        	pageRank.calculatePageRank();
    	}
    }
    
    // recommend command - training stage
    if("recommend".equalsIgnoreCase(action)) {
    	String fileName = "";
    	if (args.length != 2) {
    		System.err.println("recommend command usage is : final−project −1.0−SNAPSHOT.jar recommend movies-simple.txt");
    		System.exit(-1);
    	} else {
        	fileName = args[1];
        }
    		
        File recommendFile = new File(fileName);
        if(!recommendFile.exists()) {
          System.err.println(fileName + ": does not exist! Please check the path or contact system administrator.");
          System.exit(-1);
        }
        
        BufferedReader br = null;
    	try {
    		
    	      br = new BufferedReader(new FileReader(recommendFile));
    	      String inputFileName = br.readLine();
    	      String outputFileName = br.readLine();
    	      
    	      String line = null;
    	      ArrayList<String> userList = new ArrayList<String>();
    	      
    	      while((line = br.readLine()) != null) {
    	          	//Recommend rec = new Recommend(inputFileName,outputFileName,userList);
    	    	  //Recommend rec = new Recommend(inputFileName,outputFileName,line.trim());
    	    	  //rec.Recommendation();
    	    	  userList.add(line.trim());
    	      }
    	      
    	      
          	Recommend rec = new Recommend(inputFileName,outputFileName,userList);
          	rec.Recommendation();
    	      
    	} catch (FileNotFoundException e) {
    	      e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }

    }
    
    // commands command
    if("commands".equalsIgnoreCase(action)) {
    
    String fileName = "";
    if(args.length != 2) {
    	System.err.println("commands command usage is : final−project −1.0−SNAPSHOT.jar commands commands.txt");
		System.exit(-1);
    } else {
    	fileName = args[1];
    }
    
    File commandFile = new File(fileName);
    if(!commandFile.exists()) {
      System.err.println(fileName + ": does not exist! Please check the path or contact system administrator.");
      System.exit(-1);
    }
    
    BufferedReader br = null;
    MovieAnalyzer movAnalyzer = null;

    try {
      Method method;
      br = new BufferedReader(new FileReader(commandFile));
      String inputFileName = br.readLine();
      String outputFileName = br.readLine();
      movAnalyzer = new MovieAnalyzer(inputFileName, outputFileName);

      String line = null;
      while((line = br.readLine()) != null) {
        String[] strArr = line.trim().split(" ");
        String methodName = strArr[0];
        Object[] params = new Object[strArr.length - 1];
        Class[] clsArr = new Class[strArr.length - 1];
        for(int i = 1; i < strArr.length; i++) {
          try {
            params[i - 1] = Integer.parseInt(strArr[i].trim());
            clsArr[i - 1] = int.class;
          }
          catch(Exception e) {
            params[i - 1] = strArr[i].trim();
            clsArr[i - 1] = String.class;
          }
        }
        
        System.out.println("Invoking :: " + line);
        try {
          method = movAnalyzer.getClass().getMethod(methodName, clsArr);
          if(params.length == 0) {
            Object retObj = method.invoke(movAnalyzer);
            System.out.println("[MainRunner] " + methodName + " invocation returned " + retObj);
          }
          else if (params.length == 1) {
            Object retObj = method.invoke(movAnalyzer, params[0]);
            System.out.println("[MainRunner] " + methodName + " invocation returned " + retObj);
          }
          else if (params.length == 2) {
            Object retObj = method.invoke(movAnalyzer, params[0], params[1]);
            System.out.println("[MainRunner] " + methodName + " invocation returned " + retObj);
          }
          else if (params.length > 2) {
            throw new Exception(methodName + " does not support " + params.length + " parameters.");
          }
        }
        catch(Exception e) {
          System.err.println("ERROR Invoking " + methodName + ": " + e.getMessage());
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      try {
        if(br != null) {
          br.close();
        }
        if(movAnalyzer != null) {
          // Closing Handlers
          movAnalyzer.closeHandlers();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    }
    
    
    
  }
}
