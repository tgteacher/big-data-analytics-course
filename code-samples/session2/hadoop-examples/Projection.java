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
 * this program compute the projection based on month attribute
 * SELECT DISTINCT month from tableName
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

    /**
     * @param args first is input and second is output address
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Common.jobRunner(conf, "projection", Projection.class, ProjectionMapper.class, ProjectionReducer.class,
                ProjectionReducer.class, Text.class, Text.class, Text.class, Text.class, new Path(args[0]), new Path(args[1]));
    }
}
