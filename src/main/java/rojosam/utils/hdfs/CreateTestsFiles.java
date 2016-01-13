package rojosam.utils.hdfs;

import org.apache.hadoop.fs.*;
import rojosam.utils.NumberUtils;
import rojosam.utils.hdfs.HDFS;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by eduardo on 9/20/15.
 */
public class CreateTestsFiles {

    public static void withNumbers (int min, int max, int columns, long limit, String separator, String endOfLine, String strPath) {
        FSDataOutputStream output = null;
        try {
            FileSystem hdfs = HDFS.getHDFS();
            Path path = new Path(strPath);
            output = hdfs.create(path , true);
            boolean cont = true;
            Random randomNumber = new Random();
            while(cont){
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < columns -1; i++) {
                    sb.append(randomNumber.nextInt(max));
                    sb.append(separator);
                }
                sb.append(randomNumber.nextInt(max));
                sb.append(endOfLine);
                output.writeBytes(sb.toString());
                if(output.size() > limit){
                    cont = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                output.flush();
                output.close();
            } catch (IOException e) {
               throw new RuntimeException(e);
            }

        }
    }

    static List<String> dictionary = EnglishWords.get();

    public static void withWords(int columns, long limit, String separator, String endOfLine, String strPath) {
        FSDataOutputStream output = null;
        int max = dictionary.size();
        try {
            FileSystem hdfs = HDFS.getHDFS();
            Path path = new Path(strPath);
            System.out.println("Creating file " + path);
            output = hdfs.create(path , true);
            boolean cont = true;
            Random randomNumber = new Random();
            while(cont){
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < columns -1; i++) {
                    sb.append(dictionary.get(randomNumber.nextInt(max)));
                    sb.append(separator);
                }
                sb.append(dictionary.get(randomNumber.nextInt(max)));
                sb.append(endOfLine);
                output.writeBytes(sb.toString());
                if(output.size() > limit){
                    cont = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                output.flush();
                output.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public static void xmlWithWords(int columns, long limit, String colTag, String rowTag, String strPath) {
        FSDataOutputStream output = null;
        List<String> dictionary = EnglishWords.get();
        int max = dictionary.size();
        try {
            FileSystem hdfs = HDFS.getHDFS();
            Path path = new Path(strPath);
            output = hdfs.create(path , true);
            boolean cont = true;
            Random randomNumber = new Random();
            output.writeBytes("<root>\n");
            String colIndent = "  ";
            if(columns >1){
                colIndent = colIndent + "  ";
            }
            while(cont){
                StringBuilder sb = new StringBuilder();
                if(columns > 1){
                    sb.append("  <"+ rowTag +">\n");
                }
                for(int i = 0; i < columns -1; i++) {
                    sb.append(colIndent + "<"+ colTag + ">");
                    sb.append(dictionary.get(randomNumber.nextInt(max)));
                    sb.append("</"+ colTag + ">\n");
                }
                sb.append(colIndent + "<"+ colTag +">");
                sb.append(dictionary.get(randomNumber.nextInt(max)));
                sb.append("</" + colTag + ">\n");

                if(columns > 1){
                    sb.append("  </"+ rowTag + ">\n");
                }

                output.writeBytes(sb.toString());
                if(output.size() > limit){
                    cont = false;
                }
            }
            output.writeBytes("</root>\n");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                output.flush();
                output.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public static void multipleWithWords(int files, int columns, long limit, String separator,
                                         String endOfLine, String folder, String filePrefix) {
        try {
            FileSystem hdfs = HDFS.getHDFS();
            Path parent = new Path(folder);
            if(hdfs.exists(parent)){
                hdfs.delete(parent, true);
            }
            hdfs.mkdirs(parent);
            for(int i =0; i < files; i++){
                withWords(columns,limit,separator,endOfLine, folder + "/" + filePrefix + NumberUtils.padNum(i+1, files) + ".txt");
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    public static void localWithWords(int columns, long limit, String separator, String endOfLine, String strPath) {
        FileOutputStream fos = null;
        ZipOutputStream output = null;
        List<String> dictionary = EnglishWords.get();
        int max = dictionary.size();
        try {
            fos = new FileOutputStream(strPath);
            output = new ZipOutputStream(new BufferedOutputStream(fos));
            boolean cont = true;
            Random randomNumber = new Random();
            ZipEntry entry = new ZipEntry("test.txt");
            output.putNextEntry(entry);
            int j = 0;
            while(cont){
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < columns -1; i++) {
                    sb.append(dictionary.get(randomNumber.nextInt(max)));
                    sb.append(separator);
                }
                sb.append(dictionary.get(randomNumber.nextInt(max)));
                sb.append(endOfLine);
                output.write(sb.toString().getBytes());
                j++;
                if(j > limit){
                    cont = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                output.flush();
                output.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }

}
