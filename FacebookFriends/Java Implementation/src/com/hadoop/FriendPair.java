package com.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FriendPair implements WritableComparable<FriendPair> {

	private Friend first;
	private Friend second;

	public FriendPair(Friend first, Friend second) {
		if (first.getID().get() < second.getID().get()) {
			this.first = first;
			this.second = second;
		} else {
			this.first = second;
			this.second = first;
		}
	}

	public FriendPair() {
		this(new Friend(), new Friend());
	}

	public Friend getFirst() {
		return first;
	}

	public Friend getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	@Override
	public int compareTo(FriendPair fp) {
		// TODO Auto-generated method stub
		int cmp=getFirst().compareTo(fp.getFirst());
		if(cmp!=0){
			return cmp;
		}
		return getSecond().compareTo(fp.getSecond());
	}
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		FriendPair fp=(FriendPair)obj;
		return this.getFirst().equals(fp.getFirst())&&this.getSecond().equals(fp.getSecond());
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "["+getFirst().toString()+","+getSecond()+"]";
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.getFirst().hashCode()*163+this.getSecond().hashCode();
	}

}
