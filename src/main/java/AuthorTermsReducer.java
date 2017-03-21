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
		Set<String> dedupedFiles = new HashSet<String>( termInstancesForAuthor);
        ArrayList<String> resultsWithCount = new ArrayList<String>();
        
        for (String fileName : dedupedFiles){
        	Integer occurancesInFile = Collections.frequency( termInstancesForAuthor, fileName);
        	String filenameWithCount = String.format("\"%s\": %s", fileName, occurancesInFile.toString());
            resultsWithCount.add(filenameWithCount);
        }

		context.write(key, new Text(resultsWithCount.toString()));
	}
}