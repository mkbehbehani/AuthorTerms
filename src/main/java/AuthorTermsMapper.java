import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class AuthorTermsMapper extends Mapper<LongWritable, Text, Text, Text> {
//  extract every author in the dataset - pass 1
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] authorNames = line.split(":::")[1].split("::");
    String publicationTitle = line.split(":::")[2].split("::")[0];
    List<String> publicationTitleTerms = Arrays.asList(publicationTitle.split("\\W"));
    TermsCleaner titleCleaner = new TermsCleaner();
    List<String> cleansedTitles = titleCleaner.clean(publicationTitleTerms);

//    for each author in the line, create kv pairs of author and every word in the term list
    for (String author : authorNames) {
      for (String term : cleansedTitles) {
        if(term.length() > 2) {
          context.write(new Text(author), new Text(term));
        }
        }
    }
  }
}