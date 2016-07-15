package edu.uc.rphash;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;


public final class Centroid implements Comparable<Centroid> , java.io.Serializable{
	
	private static final long serialVersionUID = -7252059106592431985L;
	private float[] vec;
	private long count;
	public HashSet<Long> ids;
	public long id;
	public int projectionID;

	public Centroid(int dim, long id,int projectionID) {
		this.vec = new float[dim];
		this.count = 0;
		this.id = id;
		this.ids = new HashSet<Long>();
		this.projectionID = projectionID;
		ids.add(id);
	}
	
	public Centroid(int dim, long id,int projectionID,Long long1) {
		this.vec = new float[dim];
		this.count = 0;
		this.id = id;
		this.ids = new HashSet<Long>();
		this.projectionID = projectionID;
		ids.add(id);
		this.count = long1;
	}

	public Centroid(float[] data,int projectionID) {
		this.vec = data;
		this.ids = new HashSet<Long>();
		this.projectionID = projectionID;
		this.count = 1;
	}

	public Centroid(float[] data, long id,int projectionID) {
		this.vec = data;
		this.ids = new HashSet<Long>();
		ids.add(id);
		this.id = id;
		this.projectionID = projectionID;
		this.count = 1;
	}

	private void updateCentroidVector(float[] data) {
		float delta;
		count++;
		for (int i = 0; i < data.length; i++) {
			delta = data[i] - vec[i];
			vec[i] = vec[i] + delta / (float)count;
		}
	}

	public float[] centroid() {
		return vec;
	}

	public void updateVec(Centroid rp) {
		ids.addAll(rp.ids);
		float delta;
		count= count+rp.count;
		for (int i = 0; i < rp.vec.length; i++) {
			delta = rp.vec[i] - rp.vec[i];
			vec[i] = vec[i] + (rp.count*delta) / (float)count;
		}
	}
	

	public void updateVec(float[] rp) {
		updateCentroidVector(rp);
	}

	public long getCount() {
		return count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}

	public void addID(long h) {
		if (ids.size() == 0)
			id = h;
		ids.add(h);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Centroid){
			return ((Centroid)obj).ids.containsAll(ids);
		}
		return false;
	}
	
	

	@Override
	public int hashCode() {
		int hashcode = 0;
		for(Long id:ids)hashcode^=id.hashCode();
		return hashcode;
	}

	@Override
	public int compareTo(Centroid o) {
		return (int) (o.count-this.count);
	}
	
	/**
	* Always treat de-serialization as a full-blown constructor, by validating
	* the final state of the de-serialized object.
	*/
	private void readObject(ObjectInputStream aInputStream)
	throws ClassNotFoundException, IOException {
	    // always perform the default de-serialization first
	    aInputStream.defaultReadObject();
	}
	 
	/**
	* This is the default implementation of writeObject. Customise if
	* necessary.
	*/
	private void writeObject(ObjectOutputStream aOutputStream)
	throws IOException {
	    // perform the default serialization for all non-transient, non-static
	    // fields
	    aOutputStream.defaultWriteObject();
	}
}
