package pymk.pkg1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pymk1 {

    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        // Emits (a,b) *and* (b,a) any time a friend common to a and b is found.
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            // First emit all the friends of the user
            StringTokenizer st = new StringTokenizer(values.toString());
            Integer userId = Integer.parseInt(st.nextToken());
            IntWritable user = new IntWritable(userId);
            IntWritable friend1 = new IntWritable();
            ArrayList<Integer> friends = new ArrayList<>();
            // Walk through the friends in the input text
            while(st.hasMoreTokens()){
                // Emit a key-value pair for every friend found, where the value
                // is the opposite of the friend id
                Integer friend = Integer.parseInt(st.nextToken());
                friend1.set(-friend);
                context.write(user,friend1);
                // Save the friend id for later
                friends.add(friend);
            }
            // Will store the friends we've already seen as we walk through the list of friends
            ArrayList<Integer> seenFriends = new ArrayList<>(); 
            // The elements in the pairs that will be emitted.
            IntWritable friend2 = new IntWritable();
            for (Integer friend : friends) {
                friend1.set(friend);
                for (Integer seenFriend : seenFriends) {
                    friend2.set(seenFriend);
                    context.write(friend1, friend2);
                    context.write(friend2, friend1);
                }
                seenFriends.add(friend1.get());
            }
        }

    }

    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        // Models a recommendation. A recommendation has a friend
        // id and a number of friends in common.
        private static class Recommendation {

            private int friendId;
            private int nCommonFriends;

            public Recommendation(int friendId) {
                this.friendId = friendId;
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
            }

            // Getters
            
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

            // 
            
            public void addCommonFriend() {
                nCommonFriends++;
            }
            
            // String representation used in the reduce output            
            public String toString() {
                return friendId+"("+nCommonFriends+")";
            }
            
            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                return null;
            }

        }

       
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
            // Filter out the friends of key in values
            ArrayList<Integer> friendsOfKey = new ArrayList();
            ArrayList<Integer> filteredValues = new ArrayList<>();
            while (values.iterator().hasNext()){
                int value = values.iterator().next().get();
                if(value>0)
                    filteredValues.add(value);
                else
                    friendsOfKey.add(value);
            }
            for(Integer friend : friendsOfKey ){
                filteredValues.removeIf(new Predicate<Integer>() {
                    @Override
                    public boolean test(Integer t) {
                        return t.intValue() == -friend.intValue();
                    }
                });
                
            }
            ArrayList<Recommendation> recommendations = new ArrayList<>();
            // Builds the recommendation array
            for(Integer userId : filteredValues){
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } else {
                    p.addCommonFriend();
                }
            }
            // Sorts the recommendation array
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString()+" ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "people you may know");
        job.setJarByClass(Pymk1.class);
        job.setMapperClass(AllPairsMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
