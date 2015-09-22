package rojosam.process.tests;

import rojosam.utils.NumberUtils;
import rojosam.utils.hdfs.CreateTestsFiles;

/**
 * Created by eduardo on 9/20/15.
 */
public class GenerateTestFiles {

    public static void main(String[] args) {

        int bigFilesBytes = 500 * 1024 *1024;
        int oneBlockFilesBytes = 128 * 1024 * 1024 - 2000; // To avoid the last line overflow the block size
        int smallFilesBytes = 1000;
        int multipleFiles = 1000;

        System.out.println("Generating XML files. . .");
        CreateTestsFiles.xmlWithWords(1, smallFilesBytes, "col", "row", "XMLFile1.txt");
        CreateTestsFiles.xmlWithWords(10, smallFilesBytes, "col", "row", "XMLFile10.txt");

        System.out.println("Generating Word files. . .");
        CreateTestsFiles.withWords(10, bigFilesBytes, "\t", "\n", "WordsTextFile.txt");
        CreateTestsFiles.withWords(10, oneBlockFilesBytes, "\t", "\n", "WordsTextFileOneBlock.txt");
        CreateTestsFiles.multipleWithWords(multipleFiles, 10, (bigFilesBytes) / multipleFiles,
                "\t", "\n", "wordfiles", "WordsTextFile-");

        System.out.println("Generating Number files. . .");
        CreateTestsFiles.withNumbers(0, 1000, 10, bigFilesBytes, "\t", "\n", "NumberTextFile.txt");
        CreateTestsFiles.withNumbers(0, 1000, 10, oneBlockFilesBytes, "\t", "\n", "NumberTextFileOneBlock.txt");

    }
}
