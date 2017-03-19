import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class WordsByFileSumReducer extends Reducer<Text, Text, Text, Text > {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
        ArrayList<String> fileInstances = new ArrayList<String>();
		
		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (Text value : values) {
		  
		  /*
		   * Add the value to the word count counter for this key.
		   */
			fileInstances.add(value.toString());
		}
		Set<String> dedupedFiles = new HashSet<String>(fileInstances);
        ArrayList<String> resultsWithCount = new ArrayList<String>();
        
        /* Count instances of each file name in the fileInstances, use that to create a total count per file. Store in new ArrayList. */
        for (String fileName : dedupedFiles){
        	Integer occurancesInFile = Collections.frequency(fileInstances, fileName);
        	String filenameWithCount = String.format("'%s': %s", fileName, occurancesInFile.toString());
            resultsWithCount.add(filenameWithCount);
        }
		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new Text(resultsWithCount.toString()));
	}
}