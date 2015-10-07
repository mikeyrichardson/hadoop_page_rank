package net.mikeyrichardson.pagerank.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlockEntryKey implements WritableComparable<BlockEntryKey> {
    
    public IntWritable row = new IntWritable();
    public IntWritable col =  new IntWritable();
    public IntWritable source = new IntWritable();
    final public static int VECTOR_SOURCE = 0;
    final public static int MATRIX_SOURCE = 1;
 
    public BlockEntryKey(){ 
    }
    
    public BlockEntryKey(int blockRow, int blockCol, int src) {
        this.row.set(blockRow);
        this.col.set(blockCol);
        this.source.set(src);
    }

    public void write(DataOutput out) throws IOException {
        this.row.write(out);
        this.col.write(out);
        this.source.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        this.row.readFields(in);
        this.col.readFields(in);
        this.source.readFields(in);
    }
    
    public int compareTo(BlockEntryKey second) {
        if (this.row.get() == second.row.get()){
            if (this.col.get() == second.col.get()) {
                    return this.source.compareTo(second.source);
            }
            else {
                return this.col.compareTo(second.col);
            }
        }
        else{
            return this.row.compareTo(second.row);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BlockEntryKey)) {
          return false;
        }
        BlockEntryKey other = (BlockEntryKey)o;
        return this.row.get() == other.row.get() 
            && this.col.get() == other.col.get()
            && this.source.get() == other.source.get();
      }
    
    @Override
    public int hashCode() {
        return this.row.hashCode() + this.col.hashCode() + this.source.hashCode();
    }
}

