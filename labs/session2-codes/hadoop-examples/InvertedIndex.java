import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by TeamZero on 19/01/19.
 * input: inputs/word-count-input
 * output: hadoop-output/inverted-index-out/
 * local -> file:/home/mojtaba/Desktop/hadoop-examples/inputs/word-count-input
 * hadoop -> hdfs://namenode:port/[file address]
 * use same pattern for output
 */
public class InvertedIndex {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        private Text word =new Text();
        private Text doc_name = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                doc_name.set(filename);
                context.write(word, doc_name);
            }
        }
    }

    public static class InvertedReducer extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> documents = new HashSet<>();
            for(Text doc_name: values){
                documents.add(doc_name.toString());
            }
            result.set(convertSetToNormalString(documents));
            context.write(key, result);
        }
        private static String convertSetToNormalString(Set<String> documents){
            String result = "";
            Iterator<String> itr = documents.iterator();
            while(itr.hasNext()){
                result += itr.next();
                if(itr.hasNext()){
                    result += ", ";
                }
            }
            return result;
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
        Common.jobRunner(conf, "inverted index", InvertedIndex.class, TokenizerMapper.class, InvertedReducer.class, InvertedReducer.class,
                Text.class, Text.class, Text.class, Text.class, new Path(args[0]), new Path(args[1]));
    }

}
