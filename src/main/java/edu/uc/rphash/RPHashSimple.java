package edu.uc.rphash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.uc.rphash.Readers.RPHashObject;
import edu.uc.rphash.Readers.SimpleArrayReader;
import edu.uc.rphash.decoders.Decoder;
//import edu.uc.rphash.decoders.DepthProbingLSH;
import edu.uc.rphash.decoders.Leech;
import edu.uc.rphash.decoders.Spherical;
import edu.uc.rphash.frequentItemSet.ItemSet;
import edu.uc.rphash.frequentItemSet.SimpleFrequentItemSet;
import edu.uc.rphash.lsh.LSH;
import edu.uc.rphash.projections.DBFriendlyProjection;
import edu.uc.rphash.projections.Projector;
import edu.uc.rphash.standardhash.HashAlgorithm;
import edu.uc.rphash.standardhash.NoHash;
import edu.uc.rphash.tests.StatTests;
import edu.uc.rphash.tests.clusterers.KMeans2;
import edu.uc.rphash.tests.generators.GenerateData;
import edu.uc.rphash.tests.generators.GenerateStreamData;
import edu.uc.rphash.tests.kmeanspp.KMeansPlusPlus;
import edu.uc.rphash.util.VectorUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import edu.uc.rphash.Readers.StreamObject;
import edu.uc.rphash.standardhash.MurmurHash;
import edu.uc.rphash.tests.clusterers.Agglomerative3;




public class RPHashSimple implements Clusterer {

//	float variance;
	
//	public ItemSet<Long> is;
	
	List<Long> labels;
	HashMap<Long,Long> labelmap;
	
	/*public RPHashObject map() {

		// create our LSH Machine
		HashAlgorithm hal = new NoHash(so.getHashmod());
		Iterator<float[]> vecs = so.getVectorIterator();
		if (!vecs.hasNext())
			return so;

		int logk = (int) (.5 + Math.log(so.getk()) / Math.log(2));// log k and
																	// round to
																	// integer
		int k = so.getk() * logk;
		is = new SimpleFrequentItemSet<Long>(k);
		Decoder dec = so.getDecoderType();
		dec.setCounter(is);

		Projector p = so.getProjectionType();
		p.setOrigDim(so.getdim());
		p.setProjectedDim(dec.getDimensionality());
		p.setRandomSeed(so.getRandomSeed());
		p.init();
		// no noise to start with
		List<float[]> noise = LSH.genNoiseTable(
				dec.getDimensionality(),
				so.getNumBlur(),
				new Random(),
				dec.getErrorRadius()
						/ (dec.getDimensionality() * dec.getDimensionality()));

		LSH lshfunc = new LSH(dec, p, hal, noise, so.getNormalize());

		long hash;

		// add to frequent itemset the hashed Decoded randomly projected vector

		while (vecs.hasNext()) {
			float[] vec = vecs.next();
			hash = lshfunc.lshHash(vec);
			is.add(hash);
			// vec.id.add(hash);
		}
		so.setPreviousTopID(is.getTop());

		List<Float> countsAsFloats = new ArrayList<Float>();
		for (long ct : is.getCounts())
			countsAsFloats.add((float) ct);
		so.setCounts(countsAsFloats);
		return so;
	}
*/
	
	public static List<Long>[] mapphase1(int k,String inputfile) {

		SimpleArrayReader so = new SimpleArrayReader(null, k,
				RPHashObject.DEFAULT_NUM_BLUR);
		Iterator<float[]> vecs;
		try {
			vecs = new StreamObject(inputfile, 0, false)
					.getVectorIterator();
		} catch (IOException e) {
			e.printStackTrace();
			System.err
					.println("file not accessible or not found on cluster node!");
			return null;
		}
		
		float[] vec = vecs.next();
		so.setdim(vec.length);
		so.getDecoderType().setVariance(StatTests.variance(vec));                              // why is this needed ?

	//	long[] hash;
		long hash;
		
//		int projections = so.getNumProjections();
		
		// create our LSH Machine
		HashAlgorithm hal = new NoHash(so.getHashmod());
		
//		Iterator<float[]> vecs = so.getVectorIterator();
//		if (!vecs.hasNext())
//			return so;

		int logk = (int) (.5 + Math.log(so.getk()) / Math.log(2));// log k and round to integer																																
		int k1 = so.getk() * logk;
		ItemSet<Long>is = new SimpleFrequentItemSet<Long>(k1);
		Decoder dec = so.getDecoderType();
		dec.setCounter(is);

		/*Projector p = so.getProjectionType();	
		int orgdim = so.getdim();	
		p.setOrigDim(orgdim);         // getting error on spark		
		//p.setOrigDim(so.getdim());
		p.setProjectedDim(dec.getDimensionality());
		p.setRandomSeed(so.getRandomSeed());
		p.init();*/
		
		Random r = new Random(so.getRandomSeed());
		Projector p = new DBFriendlyProjection(so.getdim(),
				dec.getDimensionality(), r.nextLong());
		
		// no noise to start with
		List<float[]> noise = LSH.genNoiseTable(
				dec.getDimensionality(),
				so.getNumBlur(),
				
		//		new Random(),     // replaced with r
				r,
				(float)dec.getErrorRadius()
						/ (float)(dec.getDimensionality() * dec.getDimensionality()));

		LSH lshfunc = new LSH(dec, p, hal, noise, so.getNormalize());


		// add to frequent itemset the hashed Decoded randomly projected vector

		while (vecs.hasNext()) {
//			float[] vec = vecs.next();
			hash = lshfunc.lshHash(vec);
			is.add(hash);
			// vec.id.add(hash);
			vec = vecs.next();
		}
		so.setPreviousTopID(is.getTop());
		

		List<Float> countsAsFloats = new ArrayList<Float>();
		for (long ct : is.getCounts())
			countsAsFloats.add((float) ct);
		so.setCounts(countsAsFloats);

		//		return so;
		
		return new List[] { is.getTop(), is.getCounts() };      //// WHAT IS THE TOP VALUES ?
	}
	
	
	/*
	 * This is the second phase after the top ids have been in the reduce phase
	 * aggregated
	 */
	/*public RPHashObject reduce() {

		Iterator<float[]> vecs = so.getVectorIterator();
		if (!vecs.hasNext())
			return so;
		float[] vec = vecs.next();

		HashAlgorithm hal = new NoHash(so.getHashmod());
		Decoder dec = so.getDecoderType();

		Projector p = so.getProjectionType();
		p.setOrigDim(so.getdim());
		p.setProjectedDim(dec.getDimensionality());
		p.setRandomSeed(so.getRandomSeed());
		p.init();
		
		List<float[]> noise = LSH.genNoiseTable(so.getdim(), so.getNumBlur(),
				new Random(so.getRandomSeed()), (float)(dec.getErrorRadius())
						/ (float)(dec.getDimensionality() * dec.getDimensionality()));
		
		LSH lshfunc = new LSH(dec, p, hal, noise, so.getNormalize());
		long hash[];

		List<Centroid> centroids = new ArrayList<Centroid>();

		for (long id : so.getPreviousTopID()) {
			centroids.add(new Centroid(so.getdim(), id, -1));
		}

		this.labels = new ArrayList<>();
		
		while (vecs.hasNext()) 
		{
			hash = lshfunc.lshHashRadius(vec, noise);
			labels.add(-1l);
			//radius probe around the vector
			for (Centroid cent : centroids) {
				for (long h : hash) 
				{
					if (cent.ids.contains(h)) {
						cent.updateVec(vec);
						this.labels.set(labels.size()-1,cent.id);
					}
				}
			}
			vec = vecs.next();
		}

		Clusterer offlineclusterer = so.getOfflineClusterer();
		offlineclusterer.setData(centroids);
		offlineclusterer.setWeights(so.getCounts());
		offlineclusterer.setK(so.getk());
		this.centroids = offlineclusterer.getCentroids();
		VectorUtil.prettyPrint(this.centroids.get(0).centroid);
		this.labelmap = VectorUtil.generateIDMap(centroids,this.centroids);
		so.setCentroids(centroids);
		return so;
	}
	//271458
	//264779.7
	*/
	
	public static List<Long>[] reducephase1(List<Long>[] topidsandcounts1,
			List<Long>[] topidsandcounts2) {
		if(topidsandcounts1==null )return topidsandcounts2;
		if(topidsandcounts2==null )return topidsandcounts1;
		int k = Math.max(topidsandcounts1[0].size(), topidsandcounts2[0].size());
		k =(int) (k *Math.log(k)+.5);
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
		
		//truncate
		for(Long entry : map.keySet()) {
			retids.add(entry);
			retcounts.add(map.get(entry));
			if(count++==k)
			{
				return new List[]{retids,retcounts};
			}
		}
		return new List[]{retids,retcounts};
	}
	
	public static LinkedHashMap<Long, Long> sortByValue(
			Map<Long, Long> map) {
		
		LinkedHashMap<Long, Long> result = new LinkedHashMap<>();
		Stream<Map.Entry<Long, Long>> st = map.entrySet().stream();

		st.sorted(new Comparator<Map.Entry<Long, Long>>(){
			@Override
			public int compare(Entry<Long, Long> o1, Entry<Long, Long> o2) {
				long l1 = o1.getValue().longValue();
				long l2 = o2.getValue().longValue();
				return (int) (l2-l1);
			}	
		}).forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

		return result;
	}

	
	
	
	public static Object[] mapphase2(List<Long>[] frequentItems, String inputfile) {

		SimpleArrayReader so = new SimpleArrayReader(null,
				frequentItems[0].size(), RPHashObject.DEFAULT_NUM_BLUR);
		Iterator<float[]> vecs;
		try {
			vecs = new StreamObject(inputfile, 0, false)
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

		HashAlgorithm hal = new NoHash(so.getHashmod());
		Decoder dec = so.getDecoderType();

/*		Projector p = so.getProjectionType();
		p.setOrigDim(so.getdim());
		p.setProjectedDim(dec.getDimensionality());
		p.setRandomSeed(so.getRandomSeed());
		p.init();
		*/
		Random r = new Random(so.getRandomSeed());
		Projector p = new DBFriendlyProjection(so.getdim(),
				dec.getDimensionality(), r.nextLong());                                 // r.nextLong produces same number as in phase1 .
		
		List<float[]> noise = LSH.genNoiseTable(so.getdim(), so.getNumBlur(),
				
				//new Random(so.getRandomSeed()), (float)(dec.getErrorRadius())       // fixed to r
				
				r, (float)(dec.getErrorRadius())
						/ (float)(dec.getDimensionality() * dec.getDimensionality()));
		
		LSH lshfunc = new LSH(dec, p, hal, noise, so.getNormalize());
	

		// make a set of k default centroid objects
		List<Centroid> centroids = new ArrayList<Centroid>();

		for (int i = 0; i < frequentItems[0].size(); i++) {
			centroids.add(new Centroid(so.getdim(), frequentItems[0].get(i),
					-1, frequentItems[1].get(i)));                                // WHY THE PROJECTIONID IS -1 ?
		}

		long[] hash ;
		
     while (vecs.hasNext()) {
			
				hash = lshfunc.lshHashRadius(vec, noise);
//				labels.add(-1l);
				for (Centroid cent : centroids) {              
					for (long h : hash) {
						if (cent.ids.contains(h)) {
							cent.updateVec(vec);
							cent.addID(h);
						}
					}
				 }
			 vec = vecs.next();                          
		                                                   
        }
		
         List<float[]> centvectors = new ArrayList<float[]>();
         
         List<ConcurrentSkipListSet<Long>> centids = new ArrayList<ConcurrentSkipListSet<Long>>();
 		List<Long> centcounts = new ArrayList<Long>();
 		for (Centroid cent : centroids) {
 			centvectors.add(cent.centroid());
 			centids.add(cent.ids);
 			centcounts.add((long) cent.getCount());
 			
 		}
 		// so.setCentroids(centvectors);
 		// so.setCounts(centcounts);

 		return  new Object[]{centvectors,centids,centcounts};
 	
	}

	
	
	public static Object[] reducephase2(Object[] in1,
			Object[] in2) {
		
		int k = Math.max(((List)in1[0]).size(),((List) in2[0]).size());

		List<Centroid> cents1 = new ArrayList<Centroid>();
		
		//create a map of centroid hash ids to the centroids idx
		HashMap<Long,Integer> idsToIdx = new HashMap<>();
		for (int i = 0;i<((List)in1[0]).size();i++) 
		{
						
			Centroid vec = new Centroid((float[])((List)in1[0]).get(i),-1);
			vec.ids = (ConcurrentSkipListSet<Long>) ((List)in1[1]).get(i);
			vec.id = vec.ids.first();
			vec.setCount((long)((List)in1[2]).get(i));
			
			cents1.add(vec);
			
			for(Long id: cents1.get(i).ids)
			{
				idsToIdx.put(id, i);
			}	
		}

		// merge centroids from centroid set 2
		for (int i = 0;i<((List)in2[0]).size();i++) 
		{
			
			Centroid vec = new Centroid((float[])((List)in2[0]).get(i),-1);
			vec.ids = (ConcurrentSkipListSet<Long>) ((List)in2[1]).get(i);
			vec.id = vec.ids.first();
			vec.setCount((long)((List)in2[2]).get(i));
			
			boolean matchnotfound = true;
			for(Long id: vec.ids)
			{
				if(idsToIdx.containsKey(id))
		
				{
			
					//  cents1.get(idsToIdx.get(id)).updateVec(vec);        // modify updateVec to weighted..
					
					//   cents1.get(idsToIdx.get(id)).mergeCentroids(vec);   // tried this but got bad reults.
					
				    	 cents1.get(idsToIdx.get(id)).mergecentsspark(vec);  // same result as mergeCentroids(vec).
					
					matchnotfound = false;
				}
			}
			if(matchnotfound)cents1.add(vec);
		}
		
		Collections.sort(cents1);//reverse sort, centroid compareTo is inverted
		
		List<float[]> retcents = new ArrayList<float[]>();
		List<ConcurrentSkipListSet<Long>> retids = new ArrayList<ConcurrentSkipListSet<Long>>();
		List<Long> retcount = new ArrayList<Long>();

//		for(Centroid c : cents1.subList(0, Math.min(k,cents1.size()))){
			
		for(Centroid c : cents1.subList(0, cents1.size())){	                      // have to try
			retcents.add(c.centroid());
			retcount.add((long) c.getCount());
			retids.add(c.ids);
		}

		return new Object[]{retcents,retids,retcount};
	}
	
	
	
	
	public List<Long> getLabels()
	{
		for(int i = 0;i<labels.size();i++)
		{
			if(labelmap.containsKey(labels.get(i))){
				labels.set(i,labelmap.get(labels.get(i)));
			}
			else
			{
				labels.set(i,-1l);
			}	
		}	
		return this.labels;
	}
	
	private List<Centroid> centroids = null;
	private RPHashObject so;

	public RPHashSimple(List<float[]> data, int k) {
		so = new SimpleArrayReader(data, k, RPHashObject.DEFAULT_NUM_BLUR);
	}

	public RPHashSimple(List<float[]> data, int k, int times, int rseed) {
		so = new SimpleArrayReader(data, k);
	}

	public RPHashSimple(RPHashObject so) {
		this.so = so;
	}

	public List<Centroid> getCentroids(RPHashObject so) {
		this.so = so;
		if (centroids == null)
			try {
				run();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return centroids;
	}

	@Override
	public List<Centroid> getCentroids() {
		if (centroids == null)
			try {
				run();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return new Agglomerative3((List)so.getCentroids(), so.getk()).getCentroids();
		
	}
	

	private void run() throws IOException {
	
		String fs = "/work/deysn/rphash/data/data500.mat";
		List<Long>[] l1 = mapphase1(so.getk(),fs);
		List<Long>[] l2 = mapphase1(so.getk(),fs);
		List<Long>[] lres = reducephase1(l1,l2);

		Object[] c1 = mapphase2(lres,fs);
		Object[] c2 = mapphase2(lres,fs);

		//test serialization
		new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(c1);
		
		Object[] cres = reducephase2(c1,c2);
		
		centroids = new Agglomerative3((List) cres[0],so.getk() ).getCentroids();
		
//		System.out.println(StatTests.WCSSE((List)centroids, "/var/rphash/data/data.mat", false)); // why? is this needed?
	}	
	
	public static void main(String[] args) {
		
		
		int k = 10;
		int d = 1000;
		int n = 10000;
		float var = .6f;
		int count = 5;
		
		try {
			new RPHashSimple(null, k).run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		/*
		System.out.printf("Decoder: %s\n", "Sphere");
		System.out.printf("ClusterVar\t");
		for (int i = 0; i < count; i++)
			System.out.printf("Trial%d\t", i);
		System.out.printf("RealWCSS\n");

		for (float f = var; f < 3.01; f += .05f) {
			float avgrealwcss = 0;
			float avgtime = 0;
			System.out.printf("%f\t", f);
			for (int i = 0; i < count; i++) {
				GenerateData gen = new GenerateData(k, n / k, d, f, true, 1f);
				RPHashObject o = new SimpleArrayReader(gen.data, k);
				RPHashSimple rphit = new RPHashSimple(o);
				o.setDecoderType(new Spherical(32, 4, 1));
				//o.setDimparameter(31);
				o.setOfflineClusterer(new KMeans2());
				long startTime = System.nanoTime();
				List<Centroid> centsr = rphit.getCentroids();
				avgtime += (System.nanoTime() - startTime) / 100000000;

				//avgrealwcss += StatTests.WCSSEFloatCentroid(gen.getMedoids(),
				//		gen.getData());

				//System.out.printf("%.0f\t",
				//		StatTests.WCSSECentroidsFloat(centsr, gen.data));
				//System.gc();

			}
			System.out.printf("%.0f\n", avgrealwcss / count);
		}
*/		
	}

	@Override
	public RPHashObject getParam() {
		return so;
	}

	@Override
	public void setWeights(List<Float> counts) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setData(List<Centroid> centroids) {
		this.centroids = centroids;

	}

	@Override
	public void setRawData(List<float[]> centroids) {
		if (this.centroids == null)
			this.centroids = new ArrayList<>(centroids.size());
		for (float[] f : centroids) {
			this.centroids.add(new Centroid(f, 0));
		}
	}

	@Override
	public void setK(int getk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void reset(int randomseed) {
		centroids = null;
		so.setRandomSeed(randomseed);
	}

	@Override
	public boolean setMultiRun(int runs) {
		return true;
	}
}
