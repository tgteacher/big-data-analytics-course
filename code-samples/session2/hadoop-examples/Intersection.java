import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by TeamZero on 19/01/20.
 * input: inputs/ralational-algebra-op-input/
 * output: hadoop-output/ralational-algebra-op-out/intersection/
 * local -> file:/home/mojtaba/Desktop/hadoop-examples/inputs/ralational-algebra-op-input/
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 * this program compute the intersection based on year attribute between all documents or tables which is accessible with input argument
 */
public class Intersection {
    public static class IntersectionMapper extends Mapper<Object, Text, Text, Text>{
        private Text table_name = new Text();
        private Text attribute = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String table = Common.getTableName(context);
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String year = itr.nextToken().trim();
            attribute.set(year);
            table_name.set(table);
            context.write(attribute, table_name);
        }
    }

    public static class IntersectionReducer extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> documents = new HashSet<>();
            for(Text doc_name: values){
                documents.add(doc_name.toString());
            }
            Configuration conf = context.getConfiguration();
            int tables_count = Integer.parseInt(conf.get("tables_count"));
            if(documents.size() == tables_count){
                context.write(key, key);
            }
        }
    }

    /**
     *
     * @param args, first is input, second is output, and third is the number of
     *              table which you have in input address(we have "3" tables in our input folder)
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("tables_count", args[2]);
        Common.jobRunner(conf, "intersection", Intersection.class, IntersectionMapper.class, IntersectionReducer.class, null,
                Text.class, Text.class, Text.class, Text.class, new Path(args[0]), new Path(args[1]));

    }
}
