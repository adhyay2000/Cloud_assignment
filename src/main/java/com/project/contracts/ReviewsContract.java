package com.project.contracts;

import java.util.ArrayList;
import java.util.HashMap;

public class ReviewsContract implements Cloneable{
	private static final HashMap<String,Integer> map;
	private static final ArrayList<String> index2col;
	static{
		map = new HashMap<>();
		map.put("Id",0);
		map.put("Time",1);
		map.put("UserId",2);
		map.put("Rating",3);
		map.put("TotalVotes",4);
		map.put("HelpfulnessVotes",5);
		index2col = new ArrayList<>();
		index2col.add("Id");
		index2col.add("Time");
		index2col.add("UserId");
		index2col.add("Rating");
		index2col.add("TotalVotes");
		index2col.add("HelpfulnessVotes");
	}
	private ReviewsContract(){

	}
    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Reviews table");
    }

    static String getColumnFromIndex(int index) throws IllegalArgumentException {
        if (!(index > indToCol.size() - 1)) {
            return indToCol.get(index);
        }
        throw new IllegalArgumentException("Given column does not exist in Reviews table");
    }

    static int getNumColumns() {
        return indToCol.size();
    }

    static String getFileName() {
        return "reviews.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
