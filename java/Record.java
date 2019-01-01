import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Record implements Writable
{

    private static String DELIMETER = "\t";

    public LinkNode head = new LinkNode();
    public List<LinkNode> out_nodes = new ArrayList<>();
    public List<LinkNode> in_nodes = new ArrayList<>();

    public Record(){}

    public Record(LinkNode head)
    {
        this.head = head;
    }

    public Record(String head_link, List<String> out_links)
    {
        head = new LinkNode(head_link);
        out_nodes = new ArrayList<>();
        for(String link : out_links)
        {
            out_nodes.add(new LinkNode(link));
        }
    }

    public Record(String head_link, List<String> out_links, List<String> in_links)
    {
        head = new LinkNode(head_link);
        out_nodes = new ArrayList<>();
        for(String link : out_links)
        {
            out_nodes.add(new LinkNode(link));
        }

        in_nodes = new ArrayList<>();
        for(String link : in_links)
        {
            in_nodes.add(new LinkNode(link));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(head.toString());

        out.writeInt(out_nodes.size());
        for(LinkNode n : out_nodes)
        {
            out.writeUTF(n.toString());
        }

        out.writeInt(in_nodes.size());
        for(LinkNode n : in_nodes)
        {
            out.writeUTF(n.toString());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        head = new LinkNode();
        head.parseString(in.readUTF());

        int out_size = in.readInt();
        for(int i =0; i< out_size; i++)
        {
            LinkNode n = new LinkNode();
            n.parseString(in.readUTF());
            out_nodes.add(n);
        }

        int in_size = in.readInt();
        for(int i =0; i< in_size; i++)
        {
            LinkNode n = new LinkNode();
            n.parseString(in.readUTF());
            in_nodes.add(n);
        }
    }

    public void parseString(String in)
    {
        int idx = 0;
        String[] args = in.split(DELIMETER);

        head.parseString(args[idx++]);

        int out_size = Integer.parseInt(args[idx++]);
        for(int i =0; i < out_size; i++)
        {
            LinkNode n = new LinkNode();
            n.parseString(args[idx++]);
            out_nodes.add(n);
        }

        int in_size = Integer.parseInt(args[idx++]);
        for(int i =0; i < in_size; i++)
        {
            LinkNode n = new LinkNode();
            n.parseString(args[idx++]);
            in_nodes.add(n);
        }
    }


    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(head.toString());
        stringBuilder.append(DELIMETER);

        stringBuilder.append(out_nodes.size());
        stringBuilder.append(DELIMETER);

        for(LinkNode n : out_nodes)
        {
            stringBuilder.append(n.toString());
            stringBuilder.append(DELIMETER);
        }

        stringBuilder.append(in_nodes.size());
        stringBuilder.append(DELIMETER);

        for(LinkNode n : in_nodes)
        {
            stringBuilder.append(n.toString());
            stringBuilder.append(DELIMETER);
        }


        return stringBuilder.toString();
    }
}
