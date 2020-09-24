package com.project.utils;

public enum Tables{
	NONE(0),PRODUCTS(1),SIMILAR(2),CATEGORIES(3),REVIEWS(4);
	private final int value;
	Tables(int value){
		this.value=value;
	}
	public int getValue(){
		return value;
	}
}
