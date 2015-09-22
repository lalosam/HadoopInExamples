package rojosam.utils.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by eduardo on 9/20/15.
 */
public class EnglishWords {

    public static List<String> get(){
        List<String> dictionary = new ArrayList<>(50_000);
        ClassLoader classLoader = EnglishWords.class.getClassLoader();
        File file = new File(classLoader.getResource("american-english").getFile());
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                dictionary.add(line);
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        return dictionary;
    }

}
