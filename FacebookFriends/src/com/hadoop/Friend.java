package com.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Friend implements WritableComparable<Friend> {

	private IntWritable id;
	private Text name;
	private Text hometown;

	public Friend() {
		this(new IntWritable(), new Text(), new Text());
	}

	public Friend(IntWritable id, Text name, Text hometown) {
		this.id = id;
		this.name = name;
		this.hometown = hometown;
	}

	public Friend(int id, String name, String hometown) {
		this(new IntWritable(id), new Text(name), new Text(hometown));
	}

	public IntWritable getID() {
		return id;
	}

	public Text getName() {
		return name;
	}

	public Text getHomeTown() {
		return hometown;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		name.readFields(in);
		hometown.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		name.write(out);
		hometown.write(out);

	}

	@Override
	public int compareTo(Friend f) {
		// TODO Auto-generated method stub
		return this.getID().compareTo(f.getID());
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		Friend f=(Friend)obj;
		return this.getID().equals(f.getID());
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return String.format("%s:%s:%s", id.toString(),name.toString(),hometown.toString());
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return id.hashCode();
	}
}
