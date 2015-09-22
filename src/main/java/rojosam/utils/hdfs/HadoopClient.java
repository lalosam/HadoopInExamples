package rojosam.utils.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Created by eduardo on 9/20/15.
 */
public class HadoopClient {

    private static Configuration conf = null;

    public static Configuration getConfiguration(){
        if(conf == null) {
            conf = new Configuration();
            String hadoopConfigDir = System.getenv("HADOOP_1CONF_DIR");
            if (hadoopConfigDir == null) {
                String hadoopPrefix = System.getenv("HADOOP_PREFIX");
                if (hadoopPrefix == null) {
                    hadoopPrefix = System.getenv("HADOOP_HOME");
                }
                if (hadoopPrefix != null) {
                    hadoopConfigDir = hadoopPrefix + "/etc/hadoop";
                }
            }
            System.out.println("HADOOP Configuration:" + hadoopConfigDir);
            System.out.println("Loading configuration files . . .");
            if (hadoopConfigDir != null) {
                File configPath = new File(hadoopConfigDir);
                File[] listOfFiles = configPath.listFiles();
                for (File f : listOfFiles) {
                    if (f.getName().endsWith(".xml")) {
                        System.out.println("Loading " + f + "...");
                        conf.addResource(new Path(f.getPath()));
                    }
                }
            }
        }
        return conf;
    }
}
