package net.mikeyrichardson.pagerank.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixEntryWritable implements WritableComparable<MatrixEntryWritable> {
    
    public IntWritable row = new IntWritable();
    public IntWritable col = new IntWritable();
    public DoubleWritable value = new DoubleWritable();
 
    public MatrixEntryWritable(){ 
    }
    
    public MatrixEntryWritable(int row, int col, double value) {
        this.row.set(row);
        this.col.set(col);
        this.value.set(value);
    }

    public void write(DataOutput out) throws IOException {
        this.row.write(out);
        this.col.write(out);
        this.value.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        this.row.readFields(in);
        this.col.readFields(in);
        this.value.readFields(in);
    }
    
    public int compareTo(MatrixEntryWritable second) {
        if (this.row.get() == second.row.get()){
            return this.col.compareTo(second.col);
        }
        else{
            return this.row.compareTo(second.row);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MatrixEntryWritable)) {
          return false;
        }
        MatrixEntryWritable other = (MatrixEntryWritable) o;
        return this.row.get() == other.row.get() 
            && this.col.get() == other.col.get()
            && this.value.get() == other.value.get();
      }
    
    @Override
    public int hashCode() {
        return this.row.hashCode() + this.col.hashCode() + this.value.hashCode();
    }
}

