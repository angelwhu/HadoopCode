package hadoop;
 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class SortWordCount {
 
  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, Text >{
	 /**
	  * keyIn valueIn  keyOut valueOut 
	  */
    //private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();
 
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
 
  public static class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable> 
  {
    //private IntWritable result = new IntWritable();
 
    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
      for (Text value : values) {
    	  context.write(value, key);
      }
    }
  }
  
  public static class DescComparator extends WritableComparator 
  {
	  protected DescComparator() {  
          super(IntWritable.class,true);  
      }  

      @Override  
      public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,  
              int arg4, int arg5) {  
          return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);  
      }  
      @Override  
      public int compare(Object a,Object b){  
          return -super.compare(a, b);  
      } 
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    String[] otherArgs=new String[]{"wordCountInput","sortOutput"};
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    
    // delete output folder
    Path outputPath = new Path(otherArgs[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);
    
    job.setJarByClass(SortWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    //job.setCombinerClass(IntSumReducer.class);
    
    job.setSortComparatorClass(DescComparator.class);//set sort order
    
    job.setReducerClass(SortReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
  }
}
