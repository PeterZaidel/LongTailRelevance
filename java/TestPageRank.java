
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPageRank
{
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

    static void TestNodeWriter()
    {
        NodeWritable node1 = new NodeWritable("http://lenta.ru/news/2006/05/04/koshtinya/",
                10.00, Arrays.asList("http://www.facebook.com/2008/fbml",
                                          "http://lenta.ru/news/2014/10/27/internetpackage/"));

        String str1 = node1.toString();

        NodeWritable node2 = new NodeWritable();
        node2.parseString(str1);

        boolean res = true;

        assert (node1.getNodeUrl().equals(node2.getNodeUrl()));
        assert (node1.getRank() == node2.getRank());
        assert (node1.getLinksSize() == node2.getLinksSize());
        for(int i = 0; i < node1.getLinksSize(); i++ )
        {
            assert node1.getLinks().get(i).equals(node2.getLinks().get(i));
        }
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

        PageRankJob.N = 5;
        PageRankJob.Iterations = 2;
        PageRankJob.alpha = 0.1;
        args[0] = args[0]+"part-r-*";
        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);

    }
}
