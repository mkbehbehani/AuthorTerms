import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AuthorTermsReducer extends Reducer<Text, Text, Text, Text > {


  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
        ArrayList<String> termInstancesForAuthor = new ArrayList<String>();

		for (Text value : values) {
			 termInstancesForAuthor.add(value.toString());
		}
		Set<String> dedupedAuthors = new HashSet<String>( termInstancesForAuthor);
        ArrayList<String> resultsWithCount = new ArrayList<String>();
        
        for (String author : dedupedAuthors){
        	Integer occurencesForAuthor = Collections.frequency( termInstancesForAuthor, author);
        	String termWithCount = String.format("\"%s\": %s", author, occurencesForAuthor.toString());
            resultsWithCount.add(termWithCount);
        }

		context.write(key, new Text(resultsWithCount.toString()));
	}
}