import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NasaPageCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //RegExp to extract the html site name
    private Pattern siteExp = 
        Pattern.compile("GET\\,\\/.*\\.html");


    public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException {

      //Matcher on the current line using the regexp
      Matcher matcher = siteExp.matcher(value.toString());
		  
      //Check if the line matches
      if (matcher.find()) {
        //Set the site name without "GET," to the word
        word.set(value.toString().substring(matcher.start()+4, matcher.end()));
        //Send (word, 1) to hadoop
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, 
      Iterable<IntWritable> values, Context context) 
      throws IOException,  InterruptedException {
      
      //Compute sum of counts
      int sum = 0;
      for (IntWritable val : values)
        sum += val.get();
      
      //Send output to hadoop
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(NasaPageCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}