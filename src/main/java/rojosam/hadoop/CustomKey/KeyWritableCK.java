package rojosam.hadoop.CustomKey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by eduardo on 9/21/15.
 */
public class KeyWritableCK implements Writable, WritableComparable<KeyWritableCK>{

    private String fileName;
    private String word;

    public KeyWritableCK(){
        super();
    }

    public KeyWritableCK(String fileName, String word){
        super();
        this.fileName = fileName;
        this.word = word;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    @Override
    public int compareTo(KeyWritableCK k) {
        int result = word.compareTo(k.word);
        if (0 == result){
            result = fileName.compareTo(k.fileName);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(fileName);
        dataOutput.writeUTF(word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileName = dataInput.readUTF();
        word = dataInput.readUTF();
    }
}