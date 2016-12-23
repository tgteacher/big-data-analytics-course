/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pymk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

public class Pymk {

     public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             StringTokenizer st = new StringTokenizer(value.toString());
            IntWritable friend = new IntWritable();
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            while (st.hasMoreTokens()) {
                friend.set(Integer.parseInt(st.nextToken()));
                context.write(user,friend);
                context.write(friend,user);
            }
        }
        
    }
    
    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        private static class Pair {
            private int value;
            private int count;
            public Pair(int value){
                this.value=value;
                this.count=1;
            }
            public void increment(){
                count++;
            }
            public int getValue(){
                return value;
            }
            public int getCount(){
                return count;
            }
            public static Pair findPair(int value, ArrayList<Pair> al){
                for(Pair p : al){
                    if(p.getValue() == value)
                        return p;
                }
                return null;
            }
            
        }
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            IntWritable user = key;
            HashMap<Integer,Integer> hm = new HashMap<>();
            ArrayList<Pair> al = new ArrayList<>();
            while(values.iterator().hasNext()){
                int value = values.iterator().next().get();
                Pair p = Pair.findPair(value, al);
                if(p==null)
                    al.add(new Pair(value));
                else
                    p.increment();
            }
            al.sort(new Comparator<Pair>() {
                @Override
                public int compare(Pair t, Pair t1) {
                    return -Integer.compare(t.getCount(),t1.getCount());
                }
            });
            StringBuffer sb = new StringBuffer("");
            for(int i = 0 ; i < al.size() && i < 10 ; i ++){
                Pair p = al.get(i);
                sb.append(p.getValue()+"("+p.getCount()+") ");
            }
            Text result = new Text(sb.toString());
            context.write(user,result);
            
        }
    }
     
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "people you may know");
        job.setJarByClass(Pymk.class);
        job.setMapperClass(AllPairsMapper.class);
        job.setReducerClass(CountReducer.class);    
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
