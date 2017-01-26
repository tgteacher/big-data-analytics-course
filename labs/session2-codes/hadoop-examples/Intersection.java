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
 */
public class Intersection {
    public static class IntersectionMapper extends Mapper<Object, Text, Text, Text>{
        private Text table_name = new Text();
        private Text attribute = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String table = fileSplit.getPath().getName();
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
            Set<String> tables = convertSetToNormalString(documents);
            if(tables.size() == 3){
                context.write(key, key);
            }
        }
        private static Set<String> convertSetToNormalString(Set<String> documents){
            Set<String> result = new HashSet<>();
            Iterator<String> itr = documents.iterator();
            while(itr.hasNext()){
                result.add(itr.next());
            }
            return result;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "intersection");
        job.setJarByClass(Intersection.class);
        job.setMapperClass(Intersection.IntersectionMapper.class);
        job.setReducerClass(Intersection.IntersectionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
