package hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.SortWordCount;
import hadoop.SortWordCount.DescComparator;
import hadoop.SortWordCount.SortReducer;
import hadoop.SortWordCount.TokenizerMapper;


public class Run {
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
	    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    String[] otherArgs=new String[]{"wordInput","output"};
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job countJob = new Job(conf, "word count");
	    
	    // delete output folder
	    Path outputPath = new Path(otherArgs[1]);
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    
	    countJob.setJarByClass(Run.class);
	    countJob.setMapperClass(CountWordMapper.class);
	    countJob.setCombinerClass(CountWordReducer.class);
	    countJob.setReducerClass(CountWordReducer.class);
	    countJob.setOutputKeyClass(Text.class);
	    countJob.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(countJob, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(countJob, new Path(otherArgs[1]));
	    if(countJob.waitForCompletion(true))
	    {
	    	Configuration confSort = new Configuration();
	        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        otherArgs=new String[]{"output","sortOutput"};
	        if (otherArgs.length != 2) {
	          System.exit(2);
	        }
	        Job sortJob = new Job(confSort, "word sort");
	        
	        // delete output folder
	        outputPath = new Path(otherArgs[1]);
	        outputPath.getFileSystem(conf).delete(outputPath, true);
	        
	        sortJob.setJarByClass(Run.class);
	        sortJob.setMapperClass(SortWordMapper.class);
	        sortJob.setMapOutputKeyClass(IntWritable.class);
	        sortJob.setMapOutputValueClass(Text.class);
	        //job.setCombinerClass(IntSumReducer.class);
	        
	        sortJob.setSortComparatorClass(DescComparator.class); //set key sort order
	        //sortJob.setGroupingComparatorClass(GroupComparator.class); //set value sort order
	        
	        sortJob.setReducerClass(SortWordReducer.class);
	        sortJob.setOutputKeyClass(Text.class);
	        sortJob.setOutputValueClass(IntWritable.class);
	        
	        FileInputFormat.addInputPath(sortJob, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));
	        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
	    }
	}
}
