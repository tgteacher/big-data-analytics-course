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
 * use same pattern for output
 * this program compute the selection based on the year which is equal or greater than the condition
 * SELECT * from tableName WHERE year >= base_limit(for testing we used 1975 as a base limit)
 */
public class Selection {
    public static class SelectionMapper extends Mapper<Object, Text, Text, Text>{
        private Text record_year = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record_detail = value.toString().split(",");
            String year = record_detail[0].trim();
            Configuration conf = context.getConfiguration();
            int base_limit = Integer.parseInt(conf.get("base_limit"));
            if(Integer.parseInt(year) >= base_limit){
                record_year.set(year);
                context.write(record_year, value);
            }
        }
    }

    /**
     * @param args first is input. second is output address,
     *             and the third is the year which you want to use as a base limit(for example 1975)
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("base_limit", args[2]);
        Common.jobRunner(conf, "selection with condition", Selection.class, SelectionMapper.class, null,
                null, Text.class, Text.class, null, null, new Path(args[0]), new Path(args[1]));
    }
}
