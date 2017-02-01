import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by TeamZero on 19/01/17.
 * input: inputs/word-count-input
 * output: hadoop-output/word-count-out/
 * local -> file:/home/arezou/Desktop/hadoop-examples/inputs/word-count-input
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 */
public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable counter = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(text.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, counter);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * @param args first is input and second is output address
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Common.jobRunner(conf, "word count", WordCount.class, TokenizerMapper.class, SumReducer.class,
                SumReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Path(args[0]), new Path(args[1]));
    }

}
