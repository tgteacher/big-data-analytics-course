  import java.io.IOException;
  import java.util.StringTokenizer;
  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.IntWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.mapreduce.Job;
  import org.apache.hadoop.mapreduce.Mapper;
  import org.apache.hadoop.mapreduce.Reducer;
  import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
  import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class WordCount {

    // The class that will be used for the map tasks. Has to extend ''Mapper''.
    // In general, overrides the ``map'' method.
    // < > denotes the use of a Generic Type.  Text and IntWritable are Hadoop
    // classes used for keys and values. In Hadoop, any key or value has to
    // implement specific interfaces (Java's String or Integer cannot
    // be used directly). 
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      // Called once for each key/value pair in the input split.
      // Context is a class defined in Mapper, that gives access to
      // input and output key/value pairs.
      public void map(Object key, Text value, Context context
      ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          // Emits key/value pair in the form <word, 1>
          context.write(word, one);
        }
      }
    }

    // The class that will be used for the reducers and combiners.
    public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
      private IntWritable result = new IntWritable();
      
      public void reduce(Text key, Iterable<IntWritable> values,
      Context context
      ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
      }
    }

    // Main method
    public static void main(String[] args) throws Exception {
      // Configuration provides access to configuration parameters
      Configuration conf = new Configuration();
      // Create a new Hadoop job named ``word count''
      Job job = Job.getInstance(conf, "word count");
      // Sets the Jar by finding where the WordCount class came from
      job.setJarByClass(WordCount.class);
      // Sets mapper class for the job.
      job.setMapperClass(TokenizerMapper.class);
      // Sets combiner class for the job.
      job.setCombinerClass(IntSumReducer.class);
      // Sets reducer class for the job.
      job.setReducerClass(IntSumReducer.class);
      // Set the key class for the job output data.
      job.setOutputKeyClass(Text.class);
      // Set the value class for job outputs.
      job.setOutputValueClass(IntWritable.class);
      // Add a Path to the list of inputs for the map-reduce job.
      FileInputFormat.addInputPath(job, new Path(args[0]));
      // Set the Path of the output directory for the map-reduce job.
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }


