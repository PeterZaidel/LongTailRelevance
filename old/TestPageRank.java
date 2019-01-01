
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class TestPageRank
{
    public static int createTestLinkGraphFile(String foldername) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(foldername + "part-r-00000", "UTF-8");

//        Record nodeA = new Record("A",  Arrays.asList("B", "D", "F"), Arrays.asList("C", "D", "E"));
//        Record nodeB = new Record("B", Arrays.asList("E", "F"), Arrays.asList("A", "C"));
//        Record nodeC = new Record("C",  Arrays.asList("A", "F"), new LinkedList<String>());
//        Record nodeD = new Record("D",  Arrays.asList("A"), Arrays.asList("A"));
//        Record nodeE = new Record("E",  Arrays.asList("A"), Arrays.asList("B"));
//        Record nodeF = new Record("F",  new LinkedList<String>(), Arrays.asList("B", "A", "C"));

        Record nodeA = new Record("A",  Arrays.asList("B"), Arrays.asList("B"));
        Record nodeB = new Record("B",  Arrays.asList("A", "C"), Arrays.asList("A"));
        Record nodeC = new Record("C",  Arrays.asList(), Arrays.asList("B"));


        List<Record> nodes = Arrays.asList(nodeA, nodeB, nodeC);

        for(Record n: nodes)
        {
            //n.head.pr = 1.0 / nodes.size();
            writer.println(n.head.link + "\t" + n.toString());
        }

        writer.close();

        return nodes.size();
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

    public static void TestPR(String[] args) throws Exception {
        deleteDirectory(new File(args[1]));

        PageRankJob.Iterations = 3;
        PageRankJob.alpha = 0.1;

        PageRankJob.N = createTestLinkGraphFile(args[0]);
        PageRankJob.Reducers = 1;


        args[0] = args[0]+"part-r-*";
        int exitCode = ToolRunner.run(new PageRankJob(), args);
    }

    public static void TestHITS(String[] args) throws Exception {
        deleteDirectory(new File(args[1]));

        HITSJob.Iterations = 3;
        //HITSJob.alpha = 0.1;

        HITSJob.N = createTestLinkGraphFile(args[0]);
        HITSJob.Reducers = 1;


        args[0] = args[0]+"part-r-*";
        int exitCode = ToolRunner.run(new HITSJob(), args);
    }

    public static void main(String[] args) throws Exception {

//        TestNodeWriter();
        TestPR(args);
        //TestHITS(args);

    }
}
