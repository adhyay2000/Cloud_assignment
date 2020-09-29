package com.project.contracts;

import java.util.ArrayList;
import java.util.HashMap;

public class ProductsContracts implements Cloneable{
	private static final HashMap<String,Integer> map;
	private static final ArrayList<String> index2col;
	static{
		map = new HashMap<String,Integer>();
		map.put("Id",0);
		map.put("ASIN",1);
		map.put("Title",2);
		map.put("Group",3);
		map.put("Salesrank",4);
		map.put("NumSimilar",5);
		map.put("TotalReview",6);
		map.put("DownloadedReview",7);
		map.put("AvgRating",8);
		index2col = new ArrayList<String>();
		index2col.add("Id");
		index2col.add("ASIN");
		index2col.add("Title");
		index2col.add("Group");
		index2col.add("Salesrank");
		index2col.add("NumSimilar");
		index2col.add("TotalReview");
		index2col.add("DownloadedReview");
		index2col.add("AvgRating");
	}
	private ProductsContracts(){

	}
    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Products table");
    }

    static String getColumnFromIndex(int index) throws IllegalArgumentException {
        if (!(index > index2col.size() - 1)) {
            return index2col.get(index);
        }
        throw new IllegalArgumentException("Given column does not exist in Products table");
    }

    static int getNumColumns() {
        return index2col.size();
    }

    static String getFileName() {
        return "products.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
