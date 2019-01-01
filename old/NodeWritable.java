import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class NodeWritable implements Writable {
    private String node_url;
    private double rank;
    private List<String> node_links;

    public NodeWritable() {
        rank = 0.0f;
        node_links = new LinkedList<>();
        node_url = "";
    }

    public NodeWritable(String node_url,double rank, List<String> node_links) {
        this.rank = rank;
        this.node_links = new LinkedList<String>(node_links);
        this.node_url = node_url;
    }


    public NodeWritable(String node_url,double rank) {
        this.node_url = node_url;
        this.rank = rank;
        this.node_links = new LinkedList<String>();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(node_url);
        out.writeDouble(rank);
        out.writeInt(node_links.size());
        for (String link: node_links) {
            out.writeChars(link);
        }
    }

    public void readFields(DataInput in) throws IOException {
        node_url = in.readUTF();
        rank = in.readDouble();
        node_links = new LinkedList<>();

        int links_size = in.readInt();
        for (; links_size > 0; links_size--) {
            node_links.add(in.readUTF());
        }
    }

    public void parseString(String in)
    {
        String[] args = in.split("\t");
        node_url = args[0];
        rank = Double.valueOf(args[1]);

        node_links = new LinkedList<>();
        node_links.addAll(Arrays.asList(args).subList(2, args.length));
    }

    public String getNodeUrl()
    {
        return node_url;
    }

    public double getRank() {
        return rank;
    }

    public List<String> getLinks() {
        return node_links;
    }

    public int getLinksSize() {
        return node_links.size();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(node_url);
        stringBuilder.append("\t");
        stringBuilder.append(Double.toString(rank));
        stringBuilder.append("\t");
        for (String link : node_links) {
            stringBuilder.append(link);
            stringBuilder.append("\t");
        }

        return stringBuilder.toString();
    }
}