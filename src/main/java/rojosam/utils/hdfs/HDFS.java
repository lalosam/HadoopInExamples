package rojosam.utils.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Created by eduardo on 9/20/15.
 */
public class HDFS {

    private static FileSystem hdfs = null;

    public static FileSystem getHDFS(){
        try {
            if(hdfs == null) {
                hdfs = FileSystem.get(HadoopClient.getConfiguration());
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return hdfs;
    }


}
