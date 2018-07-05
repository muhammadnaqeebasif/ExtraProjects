package com.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class FriendArray extends ArrayWritable {

	public FriendArray(Class<? extends Writable> valueClass) {
		super(valueClass);
		// TODO Auto-generated constructor stub
	}

	public FriendArray() {
		// TODO Auto-generated constructor stub
		super(Friend.class);
	}

	public FriendArray(Class<? extends Writable> valueClass, Writable[] values) {
		super(valueClass, values);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		Friend[] friendArray=Arrays.copyOf(get(), get().length,Friend[].class);
		String result="{";
		for(Friend f:friendArray){
			result+=f.toString()+";";
		}
		result=result.substring(0, result.length()-1)+"}";
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		Friend[] f1=Arrays.copyOf(this.get(), this.get().length, Friend[].class);
		Friend[] f2=Arrays.copyOf(((ArrayWritable)obj).get(),((ArrayWritable)obj).get().length,Friend[].class);
		
		boolean result=false;
		for(Friend outerF:f1){
			result= false;
			for(Friend innerF:f2){
				if(outerF.equals(innerF)){
					result=true;
					break;
				}
			}
			if(!result){
				return result;
			}
		}
		return result;
	}

}
