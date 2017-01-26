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
 * input: inputs/ralational-algebra-op-input/users
 * output: hadoop-output/ralational-algebra-op-out/projection/
 * local -> file:/home/arezou/Desktop/hadoop-examples/inputs/ralational-algebra-op-input/users
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 */
public class Projection {
    public static class ProjectionMapper extends Mapper<Object, Text, Text, Text>{
        private Text record_month = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record_detail = value.toString().split(",");
            String month = record_detail[1].trim();
            record_month.set(month);
            context.write(record_month, record_month);
        }
    }

    public static class ProjectionReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, key);// remove duplicate
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "projection");
        job.setJarByClass(Projection.class);
        job.setMapperClass(Projection.ProjectionMapper.class);
        job.setCombinerClass(Projection.ProjectionReducer.class);
        job.setReducerClass(Projection.ProjectionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
