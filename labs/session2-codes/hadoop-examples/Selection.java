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
 * output: hadoop-output/ralational-algebra-op-out/selection/
 * local -> file:/home/arezou/Desktop/hadoop-examples/inputs/ralational-algebra-op-input/users
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for outputut
 */
public class Selection {
    public static class SelectionMapper extends Mapper<Object, Text, Text, Text>{
        private Text record_year = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record_detail = value.toString().split(",");
            String year = record_detail[0].trim();
            if(Integer.parseInt(year) >= 1975){
                record_year.set(year);
                context.write(record_year, value);
            }
        }
    }

    public static class SelectionReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "selection with condition");
        job.setJarByClass(Selection.class);
        job.setMapperClass(Selection.SelectionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
