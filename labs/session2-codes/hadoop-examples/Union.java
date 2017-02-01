import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by TeamZero on 19/01/18.
 * input: inputs/ralational-algebra-op-input/
 * output: hadoop-output/ralational-algebra-op-out/union
 * local -> file:/home/arezou/Desktop/hadoop-examples/inputs/ralational-algebra-op-input/
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 */
public class Union {
    public static class UnionMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, value);
        }
    }

    public static class UnionReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, key);
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
        Common.jobRunner(conf, "union", Union.class, UnionMapper.class, UnionReducer.class,
                UnionReducer.class, Text.class, Text.class, Text.class, Text.class, new Path(args[0]), new Path(args[1]));
    }
}
