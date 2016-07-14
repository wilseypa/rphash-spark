package edu.uc.rphash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import edu.uc.rphash.Readers.RPHashObject;
import edu.uc.rphash.Readers.SimpleArrayReader;
import edu.uc.rphash.Readers.StreamObject;
import edu.uc.rphash.decoders.Decoder;
import edu.uc.rphash.frequentItemSet.ItemSet;
import edu.uc.rphash.frequentItemSet.SimpleFrequentItemSet;
import edu.uc.rphash.lsh.LSH;
import edu.uc.rphash.projections.DBFriendlyProjection;
import edu.uc.rphash.projections.Projector;
import edu.uc.rphash.standardhash.HashAlgorithm;
import edu.uc.rphash.standardhash.MurmurHash;
import edu.uc.rphash.tests.StatTests;
import edu.uc.rphash.tests.clusterers.Kmeans;
import edu.uc.rphash.tests.generators.GenerateData;
import edu.uc.rphash.util.VectorUtil;

/**
 * This is the correlated multi projections approach. In this RPHash variation
 * we try to incorporate the advantage of multiple random projections in order
 * to combat increasing cluster error rates as the deviation between projected
 * and full data increases. The main idea is similar to the referential RPHash,
 * however the set union is projection id dependent. This will be done in a
 * simplified bitmask addition to the hash code in lieu of an array of sets data
 * structures.
 * 
 * @author lee
 *
 */
public class RPHashMultiProj implements Clusterer {
	float variance;

	/**
	 * @return
	 */
	public static List<Long>[] mapphase1(int k) {

		// Iterator<float[]> vecs = so.getVectorIterator();
		// if (!vecs.hasNext())
		// return so;

		SimpleArrayReader so = new SimpleArrayReader(null, k,
				RPHashObject.DEFAULT_NUM_BLUR);
		Iterator<float[]> vecs;
		try {
			vecs = new StreamObject("/var/rphash/data/data.mat", 0, false)
					.getVectorIterator();
		} catch (IOException e) {
			e.printStackTrace();
			System.err
					.println("file not accessible or not found on cluster node!");
			return null;
		}

		float[] vec = vecs.next();
		so.setdim(vec.length);
		so.getDecoderType().setVariance(StatTests.variance(vec));

		long[] hash;
		int projections = so.getNumProjections();

		// int k = (int) (so.getk() * projections) * 5;

		// initialize our counter
		ItemSet<Long> is = new SimpleFrequentItemSet<Long>(k);
		// create our LSH Device
		// create same LSH Device as before
		Random r = new Random(so.getRandomSeed());
		LSH[] lshfuncs = new LSH[projections];
		Decoder dec = so.getDecoderType();
		HashAlgorithm hal = new MurmurHash(so.getHashmod());

		// create same projection matrices as before
		for (int i = 0; i < projections; i++) {
			Projector p = new DBFriendlyProjection(so.getdim(),
					dec.getDimensionality(), r.nextLong());

			List<float[]> noise = LSH.genNoiseTable(dec.getDimensionality(),
					so.getNumBlur(), r,
					dec.getErrorRadius() / dec.getDimensionality());

			lshfuncs[i] = new LSH(dec, p, hal, noise);
		}

		// add to frequent itemset the hashed Decoded randomly projected vector
		while (vecs.hasNext()) {

			// iterate over the multiple projections
			for (LSH lshfunc : lshfuncs) {
				// could do a big parallel projection here
				hash = lshfunc.lshHashRadius(vec, so.getNumBlur());
				for (long hh : hash) {
					is.add(hh);
				}
			}
			vec = vecs.next();
		}

		// so.setPreviousTopID(is.getTop());
		// List<Float> countsAsFloats = new ArrayList<Float>();
		//
		// for (long ct : is.getCounts())
		// countsAsFloats.add((float) ct);
		// so.setCounts(countsAsFloats);
		return new List[] { is.getTop(), is.getCounts() };
	}

	/*
	 * This is the second phase after the top ids have been in the reduce phase
	 * aggregated
	 */
	public static List<Centroid> mapphase2(List<Long>[] frequentItems) {

		SimpleArrayReader so = new SimpleArrayReader(null,
				frequentItems[0].size(), RPHashObject.DEFAULT_NUM_BLUR);
		Iterator<float[]> vecs;
		try {
			vecs = new StreamObject("/var/rphash/data/data.mat", 0, false)
					.getVectorIterator();
		} catch (IOException e) {
			e.printStackTrace();
			System.err
					.println("file not accessible or not found on cluster node!");
			return null;
		}

		float[] vec = vecs.next();
		so.setdim(vec.length);
		so.getDecoderType().setVariance(StatTests.variance(vec));

		// make a set of k default centroid objects
		ArrayList<Centroid> centroids = new ArrayList<Centroid>();

		for (int i = 0; i < frequentItems.length; i++) {
			centroids.add(new Centroid(so.getdim(), frequentItems[0].get(i),
					-1, frequentItems[1].get(i)));
		}

		long[] hash;
		int projections = so.getNumProjections();

		// create our LSH Device
		// create same LSH Device as before
		Random r = new Random(so.getRandomSeed());
		LSH[] lshfuncs = new LSH[projections];
		Decoder dec = so.getDecoderType();
		HashAlgorithm hal = new MurmurHash(so.getHashmod());

		// create same projection matrices as before
		for (int i = 0; i < projections; i++) {
			Projector p = new DBFriendlyProjection(so.getdim(),
					dec.getDimensionality(), r.nextLong());
			List<float[]> noise = LSH.genNoiseTable(dec.getDimensionality(),
					so.getNumBlur(), r,
					dec.getErrorRadius() / dec.getDimensionality());
			lshfuncs[i] = new LSH(dec, p, hal, noise);
		}

		while (vecs.hasNext()) {
			// iterate over the multiple projections
			for (LSH lshfunc : lshfuncs) {
				// could do a big parallel projection here
				hash = lshfunc.lshHashRadius(vec, so.getNumBlur());
				for (Centroid cent : centroids) {
					for (long hh : hash) {
						if (cent.ids.contains(hh)) {
							cent.updateVec(vec);
							cent.addID(hh);
						}
					}
				}
			}
			vec = vecs.next();
		}

		List<float[]> centvectors = new ArrayList<float[]>();
		List<Long> centcounts = new ArrayList<Long>();

		for (Centroid cent : centroids) {
			centvectors.add(cent.centroid());
			centcounts.add(cent.getCount());
		}
		// so.setCentroids(centvectors);
		// so.setCounts(centcounts);

		return centroids;
	}

	private List<float[]> centroids = null;
	private RPHashObject so;

	public RPHashMultiProj(List<float[]> data, int k) {
		// variance = StatTests.varianceSample(data, .01f);
		so = new SimpleArrayReader(data, k, RPHashObject.DEFAULT_NUM_BLUR);
		// so.getDecoderType().setVariance(variance);
	}

	// public RPHashMultiProj(List<float[]> data, int k, int numProjections) {
	// variance = StatTests.varianceSample(data, .01f);
	// so.getDecoderType().setVariance(variance);
	// so = new SimpleArrayReader(data, k, 1, 2, numProjections);
	// }

	// public RPHashMultiProj(List<float[]> data, int k, int decmult,
	// int numProjections) {
	// variance = StatTests.varianceSample(data, .01f);
	// so.getDecoderType().setVariance(variance);
	// so = new SimpleArrayReader(data, k, 1, decmult, numProjections);
	// }

	public RPHashMultiProj(RPHashObject so) {
		this.so = so;
	}

	public List<float[]> getCentroids(RPHashObject so) {
		this.so = so;
		if (centroids == null)
			run();
		return centroids;
	}

	@Override
	public List<float[]> getCentroids() {

		if (centroids == null)
			run();
		return new Kmeans(so.getk(), so.getCentroids()).getCentroids();
	}

	private void run() {

		mapphase1(so.getk());
		mapphase2(new List[] { so.getPreviousTopID(), so.getCounts() });
		centroids = new Kmeans(so.getk(), so.getCentroids()).getCentroids();
	}

	public static void main(String[] args) {

		int k = 10;
		int d = 1000;
		int n = 20000;

		float var = 1.5f;
		for (float f = var; f < 4.1; f += .2f) {
			for (int i = 0; i < 1; i++) {
				GenerateData gen = new GenerateData(k, n / k, d, f, true, 1f);
				RPHashMultiProj rphit = new RPHashMultiProj(gen.data(), k);

				long startTime = System.nanoTime();
				rphit.getCentroids();
				long duration = (System.nanoTime() - startTime);
				List<float[]> aligned = VectorUtil.alignCentroids(
						rphit.getCentroids(), gen.medoids());
				System.out.println(f + ":" + StatTests.PR(aligned, gen) + ":"
						+ StatTests.WCSSE(aligned, gen.getData()) + ":"
						+ duration / 1000000000f);
				System.gc();
			}
		}
	}

	@Override
	public RPHashObject getParam() {
		return so;
	}

	public static List<Long>[] reducephase1(List<Long>[] topidsandcounts1,
			List<Long>[] topidsandcounts2) {
		if(topidsandcounts1==null )return topidsandcounts2;
		if(topidsandcounts2==null )return topidsandcounts1;
		int k = Math
				.max(topidsandcounts1[0].size(), topidsandcounts2[0].size());

		// merge lists
		HashMap<Long, Long> idsandcounts = new HashMap<Long, Long>();
		for (int i = 0; i < topidsandcounts1[0].size(); i++) {
			idsandcounts.put(topidsandcounts1[0].get(i), topidsandcounts1[1].get(i));
		}

		//merge in set 2's counts and hashes
		for (int i = 0; i < topidsandcounts2[0].size(); i++) {
			Long id = topidsandcounts2[0].get(i);
			Long count = topidsandcounts2[1].get(i);

			if (idsandcounts.containsKey(id)) {
				idsandcounts.put(id, idsandcounts.get(id) + count);
			} else {
				idsandcounts.put(id, count);
			}
		}

		// truncate list
		int count = 0;
		
		List<Long> retids    = new ArrayList<Long>();
		List<Long> retcounts = new ArrayList<Long>();
		
		LinkedHashMap<Long,Long> map = sortByValue(idsandcounts);

		//reverse ordered greatest first
		List<Map.Entry<Long, Long>> s = new ArrayList<Map.Entry<Long, Long>>(map.entrySet());		
		Collections.sort(s, Collections.reverseOrder());
		
		//truncate
		for(Map.Entry<Long, Long> entry : s) {
			retids.add(entry.getKey());
			retcounts.add(entry.getValue());
			if(count++==k)
			{
				return new List[]{retids,retcounts};
			}
		}
		
		return new List[]{retids,retcounts};
		
		
	}

	
	
	public static <K, V extends Comparable<? super V>> LinkedHashMap<K, V> sortByValue(
			Map<K, V> map) {
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		Stream<Map.Entry<K, V>> st = map.entrySet().stream();

		st.sorted(Map.Entry.comparingByValue()).forEachOrdered(
				e -> result.put(e.getKey(), e.getValue()));

		return result;
	}

	public static List<Centroid> reducephase2(List<Centroid> cents1,
			List<Centroid> cents2) {
		int k = Math.max(cents1.size(), cents2.size());

		//create a map of centroid hash ids to the centroids idx
		HashMap<Long,Integer> idsToIdx = new HashMap<>();
		for (int i = 0;i<cents1.size();i++) 
		{
			for(Long id: cents1.get(i).ids)
			{
				idsToIdx.put(id, i);
			}	
		}

		// merge centroids from centroid set 2
		for(Centroid vec : cents2)
		{
			boolean matchnotfound = true;
			for(Long id: vec.ids)
			{
				if(idsToIdx.containsKey(id))
				{
					cents1.get(idsToIdx.get(id)).updateVec(vec);
					matchnotfound = false;
					break;//break out of this for loop
				}
			}
			if(matchnotfound)cents1.add(vec);
		}
			
		//truncate the list to the top k centroids
		TreeSet<Centroid> centsAndCounts = new TreeSet<>();
		for(Centroid vec : cents1)
		{
			centsAndCounts.add(vec);
		}
		
		List<Centroid> retcentroids = new ArrayList<Centroid>();
		
		//read in reverse order
		while(!centsAndCounts.isEmpty() && retcentroids.size()<k){
			retcentroids.add(centsAndCounts.pollLast());
		}

		return retcentroids;
	}

}
