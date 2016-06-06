package univ.bigdata.course;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.lang.math.NumberUtils;
import univ.bigdata.course.mapreduce.MovieAnalyzer;

public class MainRunner {

  private static final String DEFAULT_COMMAND_FILE = "commands.txt";

  public static void main(String[] args) throws Exception {

    String commandFileName = DEFAULT_COMMAND_FILE;
    if(args.length > 0) {
      commandFileName = args[0];
    }
    File commandFile = new File(commandFileName);
    if(!commandFile.exists()) {
      System.err.println(commandFileName + ": does not exist! Please check the path or contact system administrator.");
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
          if(NumberUtils.isNumber(strArr[i].trim())){
                params[i - 1] = Integer.parseInt(strArr[i].trim());
                clsArr[i - 1] = int.class;
          }else{
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
