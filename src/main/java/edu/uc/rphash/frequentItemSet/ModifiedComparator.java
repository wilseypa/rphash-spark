package edu.uc.rphash.frequentItemSet;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;
import edu.uc.rphash.Centroid;

public class ModifiedComparator<T> implements Serializable, Comparator<Centroid> {
	ConcurrentHashMap<Long, Float> countlist;
	@Override
	public int compare(Centroid n1, Centroid n2) {
		float cn1 = countlist.get(n1.id);// count(n1.id);
		float cn2 = countlist.get(n2.id);// count(n2.id);
		int counts = (int)(cn1-cn2);
		if (counts!=0)
			return counts;
		if(n1.id!=n2.id)
			return 1;
		else
			return 0;
		}

}
