package edu.uc.rphash.Readers;



import java.util.HashMap;
//import java.util.ArrayList;
//import java.util.TreeSet;
//import java.util.stream.Stream;
//import com.google.common.collect.ArrayListMultimap;
//import com.google.common.collect.Multimap;
//import java.util.HashSet;





public class ReturnMaps {
	
	
//	public Multimap<Long, float[]> multimapIdAndCent;
//	public Multimap<Long, Long> multimapIdsAndCounts;
	
	public HashMap<Long, float[]> IDAndCent;
	public HashMap<Long, Long> IDAndCount;
	
	
	public ReturnMaps(HashMap<Long, float[]> IDAndCent ,  HashMap<Long, Long> IDAndCount ){
	
		this.IDAndCent=IDAndCent;
		
		this.IDAndCount=IDAndCount;
		
		
	
	}
	
	
	
	
	
}
