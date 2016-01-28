package edu.uc.rphash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;

import edu.uc.rphash.Readers.RPHashObject;
import edu.uc.rphash.Readers.RPVector;
import edu.uc.rphash.Readers.SimpleArrayReader;
import edu.uc.rphash.decoders.Decoder;
import edu.uc.rphash.decoders.E8;
import edu.uc.rphash.decoders.Leech;
import edu.uc.rphash.decoders.MultiDecoder;
import edu.uc.rphash.frequentItemSet.ItemSet;
import edu.uc.rphash.frequentItemSet.KHHCountMinSketch;
import edu.uc.rphash.frequentItemSet.SimpleFrequentItemSet;
import edu.uc.rphash.lsh.LSH;
import edu.uc.rphash.projections.DBFriendlyProjection;
import edu.uc.rphash.projections.Projector;
import edu.uc.rphash.standardhash.*;
import edu.uc.rphash.tests.Agglomerative;
import edu.uc.rphash.tests.GenerateData;
import edu.uc.rphash.tests.Kmeans;
import edu.uc.rphash.tests.StatTests;
import edu.uc.rphash.tests.TestUtil;

public class RPHashSimple implements Clusterer {
	float variance;

	public RPHashObject map() {

		// create our LSH Machine
		HashAlgorithm hal = new MurmurHash(so.getHashmod());
		Iterator<List<Float>> vecs = so.getVectorIterator();
		if (!vecs.hasNext())
			return so;
		
		Decoder dec = so.getDecoderType();
		Projector p = new DBFriendlyProjection(so.getdim(),
				dec.getDimensionality(), so.getRandomSeed());
		List<float[]> noise = LSH.genNoiseTable(so.getdim(), 2, new Random(), dec.getErrorRadius()/dec.getDimensionality());
		LSH lshfunc = new LSH(dec, p, hal,noise);
		long hash;
		int k = (int) (so.getk()*Math.log(so.getk()));

		ItemSet<Long> is = new SimpleFrequentItemSet<Long>(k);
		// add to frequent itemset the hashed Decoded randomly projected vector

		while (vecs.hasNext()) {
			List<Float> vecAsList = vecs.next();
			
			float[] vec = new float[vecAsList.size()];
			int j = 0;
			for (Float f : vecAsList)
				vec[j++] = (f != null ? f : Float.NaN);
			System.out.println(vec);
			hash = lshfunc.lshHash(vec);
			is.add(hash);
			//vec.id.add(hash);
		}
		so.setPreviousTopID(is.getTop());
		System.out.println(is.getTop());
//		for(long l: is.getCounts())System.out.print(l+", ");
		return so;
	}

	/*
	 * This is the second phase after the top ids have been in the reduce phase
	 * aggregated
	 */
	public RPHashObject reduce() {

		Iterator<List<Float>> vecs = so.getVectorIterator();
		if (!vecs.hasNext())
			return so;
		List<Float> vecAsList = vecs.next();
		float[] vec = new float[vecAsList.size()];
		int j = 0;
		for (Float f : vecAsList)
			vec[j++] = (f != null ? f : Float.NaN);
		
		HashAlgorithm hal = new MurmurHash(so.getHashmod());
		Decoder dec = so.getDecoderType();
		
		Projector p = new DBFriendlyProjection(so.getdim(),
				dec.getDimensionality(), so.getRandomSeed());
		List<float[]> noise = LSH.genNoiseTable(so.getdim(), 2, new Random(), dec.getErrorRadius()/(dec.getDimensionality()*dec.getDimensionality()));
		LSH lshfunc = new LSH(dec, p, hal,noise);
		long hash[];
		
		ArrayList<Centroid> centroids = new ArrayList<Centroid>();
		for (long id : so.getPreviousTopID())
			centroids.add(new Centroid(so.getdim(), id));
		System.out.println(so.getPreviousTopID() + "=====================================++++++++++++++++++++++++++++++++++");
		
		int s = 0;
		while (vecs.hasNext()) {
			hash = lshfunc.lshHashRadius(vec,noise);
			for (Centroid cent : centroids){
				int b = 0;
				for(long h:hash){
					if(cent.ids.contains(h)){
						cent.updateVec(vec);
						if(b!=0)s++;
						break;
					}
					b++;
				}
			}
			vecAsList = vecs.next();
		}

		
		for (Centroid cent : centroids) so.addCentroid(cent.centroid());
		
		return so;
	}

	private List<float[]> centroids = null;
	private RPHashObject so;

	public RPHashSimple(JavaRDD<List<Float>> dataset, int k) {
		variance = StatTests.varianceSample(dataset, .01f);
		so = new SimpleArrayReader(dataset, k);

	}

	public RPHashSimple(JavaRDD<List<Float>> dataset, int k, int times, int rseed) {
		variance = StatTests.varianceSample(dataset, .001f);
		so = new SimpleArrayReader(dataset, k);

	}

	public RPHashSimple(RPHashObject so) {
		this.so = so;
	}

	public List<float[]> getCentroids(RPHashObject so) {
		this.so=so;
		if (centroids == null)
			run();
		return new Kmeans(so.getk(),centroids).getCentroids();
	}

	@Override
	public List<float[]> getCentroids() {
		if (centroids == null)
			run();
		List<List<Float>> centroidsAsList = new ArrayList<>();
		for (float[] centroid : centroids) {
			Float[] inputBoxed = ArrayUtils.toObject(centroid);
			List<Float> inputAsList = Arrays.asList(inputBoxed);
			centroidsAsList.add(inputAsList);
		}
		System.out.println(centroidsAsList + "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
		
		return new Kmeans(so.getk(),centroids).getCentroids();
	}

	private void run() {
		
		map();
		reduce();
		centroids = so.getCentroids();
		System.out.println(so.getPreviousTopID() + "=====================================================================");
		//new Kmeans(so.getk(),so.getCentroids()).getCentroids();
	}

	/*
	public static void main(String[] args) {

		int k = 10;
		int d = 1000;
		int n = 10000;
		float var = .3f;
		for(float f = var;f<3.0;f+=.01f){
			for (int i = 0; i < 5; i++) {
				GenerateData gen = new GenerateData(k, n / k, d, f, true, 1f);
	
				RPHashSimple rphit = new RPHashSimple(gen.data(), k);
	
				long startTime = System.nanoTime();
				rphit.getCentroids();
				long duration = (System.nanoTime() - startTime);
				List<float[]> aligned = TestUtil.alignCentroids(
						rphit.getCentroids(), gen.medoids());
				System.out.println(f+":"+StatTests.PR(aligned, gen) + ":" + duration
						/ 1000000000f);
				System.gc();
			}
		}

	}
	*/

	@Override
	public RPHashObject getParam() {
		return so;
	}
}
