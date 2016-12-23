import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class HdfsPut {
  public static void main(String[] args) throws FileNotFoundException, IOException{
      String localFilePath = args[0];
      String dest = args[1];
      InputStream in = new BufferedInputStream(new FileInputStream(localFilePath));
      FileSystem fs = FileSystem.get(URI.create(dest),new Configuration());
      OutputStream out = fs.create(new Path(dest));
      IOUtils.copyBytes(in, out, 4096, true);
  }
}
