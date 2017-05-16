package hadoop.wordcount;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortWordReducer extends Reducer<IntWritable,Text, Text, IntWritable>{
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> setValues = Collections.synchronizedSet(new TreeSet<String>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				// TODO Auto-generated method stub
				return o1.compareToIgnoreCase(o2);
			}
		})); 
		for (Text value : values) 
		{
			setValues.add(value.toString());
		}
		for (String text : setValues)
		{
			context.write(new Text(text), key);
		}
		
//		Set<Text> setValues = Collections.synchronizedSet(new TreeSet<Text>(new Comparator<Text>() {
//
//			@Override
//			public int compare(Text o1, Text o2) {
//				// TODO Auto-generated method stub
//				return o1.compareTo(o2);
//			}
//		})); 
//		for (Text value : values) 
//		{
//			setValues.add(value);
//		}
//		for (Text text : setValues)
//		{
//			context.write(key, new Text(text));
//		}
			
		
//		for (Text value : values) 
//		{
//			context.write(key, value);
//		}
	}
}
