package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

public class FacebookFriendsReducer extends Reducer<FriendPair, FriendArray, FriendPair, FriendArray> {
	@Override
	protected void reduce(FriendPair key, Iterable<FriendArray> values,
			Reducer<FriendPair, FriendArray, FriendPair, FriendArray>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		List<Friend[]> list=new ArrayList<Friend[]>();
		List<Friend> commonFriend=new ArrayList<Friend>();
		int count=0;
		for(FriendArray value:values){
			Friend []toadd=Arrays.copyOf(value.get(), value.get().length, Friend[].class);
			list.add(toadd);
			count++;
		}
		if(count!=2){
			return;
		}
		for(Friend outerF:list.get(0)){
			for(Friend innerF:list.get(1)){
				if(outerF.equals(innerF)){
					commonFriend.add(innerF);
				}
			}
		}
		Friend cfArray[]=Arrays.copyOf(commonFriend.toArray(), commonFriend.toArray().length, Friend[].class);
		context.write(key, new FriendArray(Friend.class, cfArray));
	}

}
