
import java.io.*;
import java.util.*;

import javax.security.auth.login.Configuration;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class WordCountP extends Configured implements Tool{
   //Map class
	
    public static class WCMap extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line.replaceAll("\\p{Punct}", ""));

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
   
   //Reducer class
	
    public static class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
   
   //Partitioner class
	
    public static class WCPart extends Partitioner < Text, IntWritable >{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
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
    }
   
    public static void main(String ar[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setNumReduceTasks(3);

        job.setJarByClass(WordCountP.class);
        job.setMapperClass(WCMap.class);
        job.setCombinerClass(WCReduce.class);
        job.setReducerClass(WCReduce.class);
        job.setPartitionerClass(WCPart.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

