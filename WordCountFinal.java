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
                word.set(itr.nextToken().toLowerCase());
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
            String myKey = key.toString();
            if(myKey.startsWith("a") || myKey.startsWith("b") || myKey.startsWith("c")){
                return 0;
            }
            if(myKey.startsWith("d") || myKey.startsWith("e") || myKey.startsWith("f")){
                return 1;
            }
            if(myKey.startsWith("g") || myKey.startsWith("h") || myKey.startsWith("i")){
                return 2;
            }
            if(myKey.startsWith("j") || myKey.startsWith("k") || myKey.startsWith("l")){
                return 3;
            }
            if(myKey.startsWith("m") || myKey.startsWith("n") || myKey.startsWith("o")){
                return 4;
            }
            if(myKey.startsWith("p") || myKey.startsWith("q") || myKey.startsWith("r")){
                return 5;
            }
            if(myKey.startsWith("s") || myKey.startsWith("t") || myKey.startsWith("u")){
                return 6;
            }
            if(myKey.startsWith("v") || myKey.startsWith("w") || myKey.startsWith("x")){
                return 7;
            }
            if(myKey.startsWith("y") || myKey.startsWith("z")){
                return 8;
            }else{
                return 9;
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
        job.setNumReduceTasks(10);

        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true)? 0 : 1);
        return 0;
    }

    public static void main(String ar[]) throws Exception{
        int res = ToolRunner.run(new Configuration(), new WordCountFinal(),ar);
        System.exit(0);
    }
}
