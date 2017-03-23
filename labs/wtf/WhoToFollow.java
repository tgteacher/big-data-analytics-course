/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package whotofollow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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

/**
 *
 * @author Tristan Glatard tristan.glatard@creatis.insa-lyon.fr
 */
public class WhoToFollow {

    public static class IndexMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            IntWritable followee = new IntWritable();
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            while (st.hasMoreTokens()) {
                Integer tokenVal = Integer.parseInt(st.nextToken());
                followee.set(tokenVal);
                context.write(followee, user);
                followee.set(-tokenVal);
                context.write(user,followee);
            }
        }
    }

    public static class IndexReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer("");
            for (IntWritable value : values) {
                sb.append(value.toString() + " ");
            }
            context.write(key, new Text(sb.toString()));

        }
    }

    public static class CounterMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            IntWritable follower = new IntWritable();
            Integer userId = Integer.parseInt(st.nextToken()); // skips id of commonly followed user
            ArrayList<Integer> seenValues = new ArrayList<>();
            while (st.hasMoreTokens()) {
                int tokenVal = Integer.parseInt(st.nextToken());
                follower.set(tokenVal);
                if (tokenVal > 0) {
                    for (Integer seenValue : seenValues) {
                        context.write(follower, new IntWritable(seenValue));
                        context.write(new IntWritable(seenValue), follower);
                    }
                    seenValues.add(follower.get());
                } else {
                    context.write(new IntWritable(userId), new IntWritable(tokenVal));
                }
            }
        }
    }

    public static class CounterReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private static class Pair {

            private int value;
            private int count;

            public Pair(int value) {
                this.value = value;
                this.count = 1;
            }

            public void increment() {
                count++;
            }

            public int getValue() {
                return value;
            }

            public int getCount() {
                return count;
            }

            public static Pair findPair(int value, ArrayList<Pair> al) {
                for (Pair p : al) {
                    if (p.getValue() == value) {
                        return p;
                    }
                }
                return null;
            }

        }

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
            HashMap<Integer, Integer> hm = new HashMap<>();
            ArrayList<Pair> existingFollowed = new ArrayList<>();
            ArrayList<Integer> positiveValues = new ArrayList<>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                if (value < 0) {
                    existingFollowed.add(new Pair(value));
                } else {
                    positiveValues.add(value);
                }
            }

            ArrayList<Pair> al = new ArrayList<>();
            for (Integer value : positiveValues) {
                Pair p = Pair.findPair(value, al);
                Pair q = Pair.findPair(-value, existingFollowed);
                if (q == null) {
                    if (p == null) {
                        al.add(new Pair(value));
                    } else {
                        p.increment();
                    }
                }

            }
            al.sort(new Comparator<Pair>() {
                @Override
                public int compare(Pair t, Pair t1) {
                    return -Integer.compare(t.getCount(), t1.getCount());
                }
            });
            StringBuffer sb = new StringBuffer("");
            for (int i = 0; i < al.size() ; i++) {
                Pair p = al.get(i);
                sb.append(p.getValue() + "(" + p.getCount() + ") ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);

        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Indexer");
        job.setJarByClass(WhoToFollow.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Job job1 = Job.getInstance(conf, "Counter");
        job1.setJarByClass(WhoToFollow.class);
        job1.setMapperClass(CounterMapper.class);
        job1.setReducerClass(CounterReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, (new Path(args[1] + "-counter")));
        job1.waitForCompletion(true);
    }

}
