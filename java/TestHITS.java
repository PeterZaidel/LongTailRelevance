import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestHITS {
    public static void createTestLinkGraphFile(String foldername) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(foldername + "part-r-00000", "UTF-8");

        Record nodeA = new Record("A",  Arrays.asList("B", "D"), Arrays.asList("C", "D", "E"));
        Record nodeB = new Record("B", Arrays.asList("E"), Arrays.asList("A"));
        Record nodeC = new Record("C",  Arrays.asList("A"), new ArrayList<String>());
        Record nodeD = new Record("D",  Arrays.asList("A"), Arrays.asList("A"));
        Record nodeE = new Record("E",  Arrays.asList("A"), Arrays.asList("B"));

        List<Record> nodes = Arrays.asList(nodeA, nodeB, nodeC,
                nodeD, nodeE);

        for(Record n: nodes)
        {
            writer.println(n.head.link + "\t" + n.toString());
        }

        writer.close();
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void main(String[] args) throws Exception {

//        TestNodeWriter();

        deleteDirectory(new File(args[1]));


        createTestLinkGraphFile(args[0]);

        HITSJob.N = 5;
        HITSJob.Iterations = 3;

        args[0] = args[0]+"part-r-*";
        int exitCode = ToolRunner.run(new HITSJob(), args);
        System.exit(exitCode);

    }
}
