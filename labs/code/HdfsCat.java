import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class HdfsCat {

    public static void main(String[] args) throws IOException {
        // URI to 'cat' will be passed as first argument
        String hdfsUri = args[0];
        // Create the FileSystem object
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
        // Create HDFS Path from URI
        Path path = new Path(hdfsUri);
        InputStream in = null;
        try{
            // Open an InputStream from the Path
            in = fs.open(path);
            // Copy bytes from InputStream to stdout
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally{
            // Close the InputStream
            IOUtils.closeStream(in);
        }
    }  
}
