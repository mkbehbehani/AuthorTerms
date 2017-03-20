import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class WordsByFileWordMapper extends Mapper<LongWritable, Text, Text, Text> {
//  extract every author in the dataset - pass 1



  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    String line = value.toString();
    String[] authorNames = line.split(":::")[1].split("::");

    /*
     * The line.split("\\W+") call uses regular expressions to split the
     * line up by non-word characters.
     * 
     * If you are not familiar with the use of regular expressions in
     * Java code, search the web for "Java Regex Tutorial." 
     */
    for (String word : authorNames) {
      if (word.length() > 0) {
        MapWritable result = new MapWritable();
        Writable currentFile = new Text(fileName);
        Writable currentCount = new IntWritable(1);
        result.put(currentFile, currentCount);
		/*
         * Call the write method on the Context object to emit a key
         * and a value from the map method.
         */
        context.write(new Text(word), new Text(fileName));
      }
    }
  }
}