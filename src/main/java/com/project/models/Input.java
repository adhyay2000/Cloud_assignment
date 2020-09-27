package com.project.models;
import java.util.ArrayList;
import java.util.StringTokenizer;
import com.project.utils.AggFunc;
import com.project.utils.Tables;
public class Input{
	private String select_part;
	private String from_part;
	private String where_part;
	private String groupBy_part;
	private String having_part;
	private Tables table;
	private int comparisonNumber;
	private int fnCol;
	private AggFunc fn;
	private ArrayList<int> columns;
	public Input(String select,String from,String where,String groupBy,String having){
		select_part=select;
		from_part=from;
		where_part = where;
		groupBy_part=groupBy;
		having_part=having;
		selection_col = new ArrayList<>();
		table = TABLES.NONE;
		fn=AggFunc.NONE;
		comparisonNumber = -1;
		group_col = new ArrayList<>();
	}
	public void parse(){
		if(from_part=="PRODUCTS"){
			table=TABLES.PRODUCTS;
		}else if(from_part=="SIMILAR"){
			table=TABLES.SIMILAR;
		}else if(from_part=="CATEGORIES"){
			table=TABLES.CATEGORIES;
		}else if(from_part=="REVIEWS"){
			table=TABLES.REVIEWS;
		}
		StringTokenizer st = new StringTokenizer(select_part,",");
		//Maintain hashMap for string to index mapping
		while(st.hasMoreTokens()){
			String column = st.nextToken();
			if(st.hasMoreTokens()==True){
				selection_col.add(findIndex(table,column));
			}else{ //isLast
				String tmp;
				if(column.startsWith("COUNT")){
					fn=AggFunc.COUNT;
					tmp = column.substring(5);
				}else if(column.startsWith("SUM")){
					fn=AggFunc.SUM;
					tmp = column.substring(3);
				}else if(column.startsWith("MAX"){
					fn=AggFunc.MAX;
					tmp=column.substring(3);
				}else if(column.startsWith("MIN"){
					fn=AggFunc.MIN;
					tmp=column.substring(3);
				}
				selection_func_col = findIndex(table,tmp);
			}
		}
		st = new StringTokenizer(groupBy_part,",");
		while(st.hasMoreTokens()){
			String column = st.nextToken();
			groupBy.add(findIndex(table,column));
		}
	}
	public Table getTable(){
		return table;
	}
	public AggFunc getAggFunc(){
		return fn;
	}
	public int getComparisonNumber(){
		return comparisonNumber;
	}
	public ArrayList<int> getcolumns(){
		return columns;
	}
	public int getFnCol(){
		return fnCol;
	}
}
