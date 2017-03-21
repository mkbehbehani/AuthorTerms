import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

public class AuthorTermsMapper extends Mapper<LongWritable, Text, Text, Text> {
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
    String line = value.toString();
    String[] authorNames = line.split(":::")[1].split("::");
    String publicationTitle = line.split(":::")[2].split("::")[0];
    List<String> publicationTitleTerms = Arrays.asList(publicationTitle.split("\\W"));
    TermsCleaner titleCleaner = new TermsCleaner();
    titleCleaner.clean(publicationTitleTerms);

//    for each author in the line create kv pairs of author and every word in the term list
    for (String author : authorNames) {
      for (String term : publicationTitleTerms) {
        context.write(new Text(author), new Text(term));
        }
    }
  }
}