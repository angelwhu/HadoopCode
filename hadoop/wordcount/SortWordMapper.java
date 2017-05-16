package hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortWordMapper extends Mapper<Object, Text, IntWritable, Text >{
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      // Default TextInputFormat. key is line number, value is content.
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      Text word = new Text();
      IntWritable count = new IntWritable();
      while (itr.hasMoreTokens()) {
    	
        word.set(itr.nextToken());
        count.set(Integer.parseInt(itr.nextToken()));
        context.write(count, word);
      }
    }

}
