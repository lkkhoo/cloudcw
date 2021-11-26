import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.w3c.dom.Text;

public class WordCountFinal extends Configured implements Tool{

    //Mapper Class
    public static class WCMap extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                 
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line.replaceAll("\\p{Punct}", ""));

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    
    //Reducer Class
    public static class WCRed extends Reducer<Text,IntWritable,Text,IntWritable> {
 
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

    public static class WCPart extends Partitioner<Text,IntWritable>{
        
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String myKey = key.toString().toLowerCase();
            if(myKey.startsWith("a")){
                return 0;
            }
            if(myKey.startsWith("b")){
                return 1;
            }else{
                return 2;
            }
        }
    }

    @Override
    public int run(String[] arg) throws Exception{
        Configuration conf = getConf();
        Job job = new Job(conf, "word count");

        job.setJarByClass(WordCountFinal.class);

        FileInputFormat.setInputPaths(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job,new Path(arg[1]));

        job.setMapperClass(WCMap.class);

        job.setPartitionerClass(WCPart.class);
        job.setCombinerClass(WCRed.class);
        job.setReducerClass(WCRed.class);
        job.setNumReduceTasks(3);

        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        System.exit(job.waitForCompletion(true)? 0 : 1);
        return 0;
    }

    public static void main(String ar[]) throws Exception{
        int res = ToolRunner.run(new Configuration(), new WordCountFinal(),ar);
        System.exit(0);
    }
}
