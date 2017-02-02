import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/**
 * Created by TeamZero on 23/01/20.
 * input: inputs/ralational-algebra-op-input/
 * output: hadoop-output/ralational-algebra-op-out/difference/
 * table name: employee
 * local -> file:/home/mojtaba/Desktop/hadoop-examples/inputs/ralational-algebra-op-input/
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 * this program compute the difference based on year attribute from selected table which is defined as third argument of main method
 * SELECT year from employee WHERE year NOT IN (SELECT year from other_tables)
 */
public class Difference {

    public static class DifferenceMapper extends Mapper<Object , Text, Text, Text>{
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
    public static class DifferenceReducer extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> documents = new HashSet<>();
            for(Text doc_name: values){
                documents.add(doc_name.toString());
            }
            Configuration conf = context.getConfiguration();
            String tbl_name = conf.get("table_name");
            if(documents.size() == 1 && documents.contains(tbl_name)){
                result.set(tbl_name);
                context.write(key, result);
            }
        }
    }

    /**
     * @param args input address, output address, and selected table name(we used "employee" for testing)
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("table_name", args[2]);
        Common.jobRunner(conf, "difference", Difference.class, DifferenceMapper.class, DifferenceReducer.class, null,
                Text.class, Text.class, Text.class, Text.class, new Path(args[0]), new Path(args[1]));
    }
}
