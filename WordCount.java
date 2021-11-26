import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class WCMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
					
	    String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line.replaceAll("\\p{Punct}", ""));

      //Tokenised strings contain punctuations, e.g. comma, and fullstop
      //Your task is to ensure that only words themselves are used as keys

      while (itr.hasMoreTokens()) {
	      word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class WCPartitioner implements Partitioner<Text, IntWritable>{

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions){
      String myKey = key.toString().toLowerCase();
      if (myKey.startsWith("a")){
        return 0;
      }
      if (myKey.startsWith("b")){
        return 1;
      }else{
        return 2;
      }
    }

    @Override
    public void configure(JobConf arg0){
      
    }
  }

  public static class WCReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");

    job.setNumReduceTasks(3);

    job.setJarByClass(WordCount.class);
    job.setMapperClass(WCMapper.class);
    job.setReducerClass(WCReducer.class);
    job.setPartitionerClass(WCPartitioner.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}