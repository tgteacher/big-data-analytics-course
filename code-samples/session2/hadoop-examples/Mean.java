import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * Created by TeamZero on 23/01/19.
 * input: inputs/mean-input/
 * output: hadoop-output/mean-out/
 * local -> file:/home/mojtaba/Desktop/hadoop-examples/inputs/mean-input/
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 */
public class Mean {
    public static class MeanMapper extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text term = new Text();
            IntWritable cost = new IntWritable();
            StringTokenizer itr = new StringTokenizer(value.toString());
            term.set(itr.nextToken());
            cost.set(Integer.parseInt(itr.nextToken()));
            context.write(term, cost);
        }
    }

    public static class MeanReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int counter = 0;
            for(IntWritable value: values){
                counter++;
                sum += value.get();
            }
            double avg = sum/ counter;
            DoubleWritable result = new DoubleWritable();
            result.set(avg);
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
        Common.jobRunner(conf, "mean", Mean.class, MeanMapper.class, MeanReducer.class, null,
                Text.class, IntWritable.class, Text.class, DoubleWritable.class, new Path(args[0]), new Path(args[1]));
    }
}
