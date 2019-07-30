package com.dtdb.master.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class MultiSUMSeperator {
	public static List<String> items(String str, String seperator) {
		List<String> items = new ArrayList<String>();
		Integer index = 0;
		for(int i=0;i<=5;i++){
			Item item = getItems(str, index, seperator);
			index = item.lastIndex;
			if(item.subString==null) break;
			items.add(item.subString);
		}
		
		return items;
	}
	
	private static Item getItems(String str, Integer startIndex, String seperator){
		Item item = new Item();
		String subString;
		Integer indexOf = StringUtils.indexOf(str, seperator, startIndex);
		if(indexOf>-1){
			Integer indexOfClosure = StringUtils.indexOf(str, ")", indexOf);
			subString = str.substring(indexOf, indexOfClosure+1);
			item.lastIndex = indexOfClosure;
			item.subString = subString;
		}
		
		return item;
	}
	
}

class Item{
	String subString;
	Integer lastIndex;
}