package writables;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TextTextLong implements WritableComparable<TextTextLong> {
    private Text first; // Host
    private Text second;    // Query
    private LongWritable third; // Freq
    private final String splitter = "<SPLITTER>";

    public TextTextLong() {
        set(new Text(), new Text(), new LongWritable());
    }


    public TextTextLong(Text first, Text second, LongWritable third) {
        set(first, second, third);
    }

    public TextTextLong(String s){
        String[] split = s.split(splitter);
        if (split.length == 3){
            set(new Text(split[0]), new Text(split[1]), new LongWritable(Long.parseLong(split[2])));
        }
    }

    private void set(Text a, Text b, LongWritable c) {
        first = a;
        second = b;
        third = c;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public LongWritable getThird() {
        return third;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        third.write(out);
    }

    @Override
    public int compareTo(@Nonnull TextTextLong o) {
        int cmp1 = first.compareTo(o.first); // host
        int cmp2 = second.compareTo(o.second); // query
        int cmp3 = -third.compareTo(o.third); // freq
        if (!(cmp1 == 0)){
            return cmp1;
        }
        else if (!(cmp3 == 0)){
            return cmp3;
        }
        else return cmp2;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
        third.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode() * 63 + third.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextTextLong) {
            TextTextLong tp = (TextTextLong) obj;
            return first.equals(tp.first) && second.equals(tp.second) && third.equals(tp.third);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + splitter + second + splitter + third.toString();
    }
}