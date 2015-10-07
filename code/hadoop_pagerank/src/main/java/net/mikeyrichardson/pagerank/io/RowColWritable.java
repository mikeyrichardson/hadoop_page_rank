package net.mikeyrichardson.pagerank.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RowColWritable implements WritableComparable<RowColWritable> {
    
    public IntWritable row = new IntWritable();
    public IntWritable col = new IntWritable();
 
    public RowColWritable(){ 
    }
    
    public RowColWritable(int row, int col) {
        this.row.set(row);
        this.col.set(col);
    }

    public void write(DataOutput out) throws IOException {
        this.row.write(out);
        this.col.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        this.row.readFields(in);
        this.col.readFields(in);
    }
    
    public int compareTo(RowColWritable second) {
        if (this.row.get() == second.row.get()){
            return this.col.compareTo(second.col);
        }
        else{
            return this.row.compareTo(second.row);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RowColWritable)) {
          return false;
        }
        RowColWritable other = (RowColWritable) o;
        return this.row.get() == other.row.get() 
            && this.col.get() == other.col.get();
      }
    
    @Override
    public int hashCode() {
        return this.row.hashCode() + this.col.hashCode();
    }
}

