package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sange on 16/04/16.
 */
public class StringLineWritable implements WritableComparable<StringLineWritable> {
    private String line;

    public StringLineWritable() {
    }

    public String get() {
        return line;
    }

    public void set(String line) {
        this.line = line;
    }

    @Override
    public int compareTo(StringLineWritable o) {
        return this.line.compareTo(o.line);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.line);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.line = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringLineWritable that = (StringLineWritable) o;

        return line != null ? line.equals(that.line) : that.line == null;

    }

    @Override
    public int hashCode() {
        return line != null ? line.hashCode() : 0;
    }

    @Override
    public String toString() {
        return line;
    }
}
