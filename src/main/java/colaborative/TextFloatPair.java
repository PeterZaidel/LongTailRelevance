package colaborative;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TextFloatPair implements WritableComparable<TextFloatPair> {


    public static class mPartitioner extends Partitioner<TextFloatPair, IntWritable> {
        @Override
        public int getPartition(TextFloatPair key, IntWritable val, int numPartitions) {
            // Для каждой метеостанции в конкретный день месяца - отправляем на один reducer
            return Math.abs(key.getFirst().hashCode()) % numPartitions;
        }
    }

    public static class mKeyComparator extends WritableComparator {
        protected mKeyComparator() {
            super(TextFloatPair.class, true /* десериализовывать ли объекты (TextFloatPair) для compare */);
        }

        /*
         * Сортируем по полному ключу:
         * - id метеостанции
         * - день и месяц
         * - если совпали - по убыванию температуры (см TextFloatPair.compareTo)
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextFloatPair)a).compareTo((TextFloatPair)b);
        }
    }

    public static class mGrouper extends WritableComparator {
        protected mGrouper() {
            super(TextFloatPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // считаем за группу текстовую часть ключа: {id станции, день, месяц}
            Text a_first = ((TextFloatPair)a).getFirst();
            Text b_first = ((TextFloatPair)b).getFirst();
            return a_first.compareTo(b_first);
        }
    }


    private Text first;
    private FloatWritable second;

    public TextFloatPair() {
        set(new Text(), new FloatWritable());
    }

    public TextFloatPair(String first, float second) {
        set(new Text(first), new FloatWritable(second));
    }

    public TextFloatPair(Text first, FloatWritable second) {
        set(first, second);
    }

    private void set(Text a, FloatWritable b) {
        first = a;
        second = b;
    }

    public Text getFirst() {
        return first;
    }

    public FloatWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(@Nonnull TextFloatPair o) {
        int cmp = first.compareTo(o.first);
        return (cmp == 0) ? -second.compareTo(o.second) : cmp;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextFloatPair) {
            TextFloatPair tp = (TextFloatPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}