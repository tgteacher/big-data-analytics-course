package kmeans;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {

    /*****************/
    /**** Mapper  ****/
    /*****************/
    public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        private ArrayList<Point2D.Double> centroids = new ArrayList<>();
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Read the file containing centroids
            URI centroidURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
            Path centroidsPath = new Path(centroidURI.getPath());
            BufferedReader br = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
            String centroid = null;
            while ((centroid = br.readLine()) != null) {
                String[] splits = centroid.split("\t")[1].split(" ");
                centroids.add(new Point2D.Double(
                        Double.parseDouble(splits[0]),
                        Double.parseDouble(splits[1])
                ));
            }
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             // Emit (i,value) where i is the id of the closest centroid
             String[] splits = value.toString().split(" ");
             Point2D.Double point = new Point2D.Double(
                     Double.parseDouble(splits[0]),
                     Double.parseDouble(splits[1])
             );
             double minDistance = centroids.get(0).distance(point);
             Point2D.Double closestCentroid = centroids.get(0);
             for(Point2D.Double centroid : centroids){
                 double distance = centroid.distance(point);
                 if(distance < minDistance){
                     minDistance = distance;
                     closestCentroid = centroid;
                 }
             }
             context.write(new IntWritable(centroids.indexOf(closestCentroid)),
                           new Text(point.getX()+ " " + point.getY()));
         }
    }

    /**************/
    /** Reducer  **/
    /**************/
    public static class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
      public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          Point2D.Double point = new Point2D.Double(0,0);
          int nPoints = 0;
          while(values.iterator().hasNext()){
              nPoints++;
              String[] pointString = values.iterator().next().toString().split(" ");
              point.setLocation(point.getX()+Double.parseDouble(pointString[0]),
                                point.getY()+Double.parseDouble(pointString[1]));
          }
          point.setLocation(point.getX()/nPoints,
                            point.getY()/nPoints);
          context.write(key, new Text(point.getX()+ " " + point.getY()));
      }
    }

    /************/
    /**  Main  **/
    /************/
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        // Arguments parsing
        String inputPath = args[0];
        String baseOutputPath = args[1];
        int nClusters = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);
        
        // Centroid initialization
        ArrayList<String> centroids = new ArrayList<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(inputPath));
            String line = null;
            for (int i = 0; i < nClusters; i++) {
                centroids.add(br.readLine());
            }
        } finally {
            br.close();
        }
        File f = File.createTempFile("centroids", ".txt");
        FileWriter fw = null;
        try {
            fw = new FileWriter(f);
            int i =0;
            for(String s : centroids){
                fw.write(i+++"\t"+s+"\n");
            }
        } finally {
            fw.close();
        }

        // Iterations
        String centroidFile = f.getAbsolutePath();
        Configuration conf = new Configuration();
        String oldDigest = null;
        for (int i = 0; i < maxIterations; i++) {
            Job job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            String outputPath = baseOutputPath + "-" + i;
            // Set job output
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            // Distribute the centroid file to the mappers using the distributed cache
            job.addCacheFile(new URI(centroidFile));
            // Run the job
            if(!job.waitForCompletion(true)) // job failed
                System.exit(1);
            // Prepare for the next iteration
            centroidFile = outputPath + "/part-r-00000";
            // Compute a hash of the centroid file
            byte[] b = Files.readAllBytes(Paths.get(centroidFile));
            byte[] hash = MessageDigest.getInstance("MD5").digest(b);
            String newDigest = DatatypeConverter.printHexBinary(hash);
            if(newDigest.equals(oldDigest))
                break; // algorithm converged
            oldDigest = newDigest;
        }
        // Do a final map step to output the classification
        Job job = Job.getInstance(conf, "kmeans");
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String outputPath = baseOutputPath + "-classification";
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.addCacheFile(new URI(centroidFile));
         if(!job.waitForCompletion(true)) // job failed
                System.exit(1);
         else
             System.exit(0);
    }
}
