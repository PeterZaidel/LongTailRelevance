import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class LinkNode implements Writable {

    public static String DELIMETER = ":";

    public String link = "";
    public double pr = 0.0;
    public double a = 1.0;
    public double h = 1.0;


    public LinkNode()
    {}

    public LinkNode(String link)
    {
        this.link = link;
    }

    public LinkNode(String link, double pr, double a, double h)
    {
        this.link = link;
        this.pr = pr;
        this.a = a;
        this.h = h;
    }



    public void write(DataOutput out) throws IOException {
        out.writeUTF(link);
        out.writeDouble(pr);
        out.writeDouble(a);
        out.writeDouble(h);
    }

    public void readFields(DataInput in) throws IOException {
        link = in.readUTF();
        pr = in.readDouble();
        a = in.readDouble();
        h = in.readDouble();
    }

    public void parseString(String in)
    {
        String[] args = in.split(DELIMETER);
        link = args[0];
        pr = Double.parseDouble(args[1]);
        a = Double.parseDouble(args[2]);
        h = Double.parseDouble(args[3]);
    }

    public String getLink()
    {
        return link;
    }

    public double getPR() {
        return pr;
    }
    public double getA(){return a;}
    public double getH(){return h;}


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(link);
        stringBuilder.append(DELIMETER);

        stringBuilder.append(pr);
        stringBuilder.append(DELIMETER);

        stringBuilder.append(a);
        stringBuilder.append(DELIMETER);

        stringBuilder.append(h);
        stringBuilder.append(DELIMETER);

        return stringBuilder.toString();

    }
}