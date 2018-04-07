package edu.uc.rphash;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Stream;

import edu.uc.rphash.Readers.RPHashObject;
import edu.uc.rphash.Readers.SimpleArrayReader;
import edu.uc.rphash.Readers.StreamObject;
import edu.uc.rphash.projections.DBFriendlyProjection;
import edu.uc.rphash.projections.Projector;
import edu.uc.rphash.tests.StatTests;
import edu.uc.rphash.tests.clusterers.Agglomerative3;
import edu.uc.rphash.tests.generators.GenerateData;


public class RPHashAdaptive2Pass implements Clusterer, Runnable {

//	static boolean znorm = true;              // should this be static ??
	static boolean znorm = false; 
	
	private int counter;                       // what is the need for this?
	private static float[] rngvec;             // should this be static ??
	private List<Centroid> centroids = null;
	private RPHashObject so;

	public RPHashAdaptive2Pass(RPHashObject so) {
		this.so = so;
	}

	public List<Centroid> getCentroids(RPHashObject so) {
		this.so = so;
		return getCentroids();
	}

	@Override
	public List<Centroid> getCentroids() {
		if (centroids == null)
			run();
		return centroids;
	}

	/*
	 * X - set of vectors compute the medoid of a vector set
	 */
	float[] medoid(List<float[]> X) {
		float[] ret = X.get(0);
		for (int i = 1; i < X.size(); i++) {
			for (int j = 0; j < ret.length; j++) {
				ret[j] += X.get(i)[j];
			}
		}
		for (int j = 0; j < ret.length; j++) {
			ret[j] = ret[j] / ((float) X.size());
		}
		return ret;
	}

	//float[] rngvec; the range vector is moot if incoming data has been normalized
	//post normalization it should all be zero centered, with variance 1


	/*
	 * super simple hash algorithm, reminiscient of pstable lsh
	 */
	
	
	
	public static  long hashvec(float[] xt, float[] x,                                 // should this be static ??
			HashMap<Long, List<float[]>> IDAndCent, HashMap<Long, List<Integer>> IDAndLabel,int ct) {
		long s = 1;//fixes leading 0's bug
		for (int i = 0; i < xt.length; i++) {
			s <<= 1;
			if (xt[i] > rngvec[i])                                // err
				s += 1;
			if (IDAndCent.containsKey(s)) {
				IDAndLabel.get(s).add(ct);
				IDAndCent.get(s).add(x);
			} else {
				List<float[]> xlist = new ArrayList<>();
				xlist.add(x);
				IDAndCent.put(s, xlist);
				List<Integer> idlist = new ArrayList<>();
				idlist.add(ct);
				IDAndLabel.put(s, idlist);
			}
		}
		return s;
	}
	

	/*
	 * x - input vector IDAndCount - ID->count map IDAndCent - ID->centroid
	 * vector map
	 * 
	 * hash the projected vector x and update the hash to centroid and counts
	 * maps
	 */
	static void addtocounter(float[] x, Projector p,                                                    // should this be static ??
			HashMap<Long, List<float[]>> IDAndCent,HashMap<Long, List<Integer>> IDandID,int ct) {
		float[] xt = p.project(x);
		
//		counter++;    
//		for(int i = 0;i<xt.length;i++){
//			float delta = xt[i]-rngvec[i];
//			rngvec[i] += delta/(float)counter;
//		}
		
		 hashvec(xt,x,IDAndCent, IDandID,ct);                   // returns a long ?.              //err
	}
	
	static void addtocounter(float[] x, Projector p,													// should this be static ??
			HashMap<Long, List<float[]>> IDAndCent,HashMap<Long, List<Integer>> IDandID,int ct,float[] mean,float[] variance)
	{
		float[] xt = p.project(StatTests.znormvec(x, mean, variance));
		
//		counter++;    
//		for(int i = 0;i<xt.length;i++){
//			float delta = xt[i]-rngvec[i];
//			rngvec[i] += delta/(float)counter;
//		}
		
		hashvec(xt,x,IDAndCent, IDandID,ct);
	}

	static boolean isPowerOfTwo(long num) {
		return (num & -num) == num;
	}

	
	
	
	/*
	 * X - data set k - canonical k in k-means l - clustering sub-space Compute
	 * density mode via iterative deepening hash counting
	 */

	
		
		public static HashMap<Long, List<float[]>>  findDensityModesPart_1(int k,String inputfile ) {
		HashMap<Long, List<float[]>> IDAndCent = new HashMap<>();
		HashMap<Long, List<Integer>> IDAndID = new HashMap<>();          // why is this needed ?
		
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
		
		float[] vec = vecs.next();                            // need this ?
		
		
		rngvec = new float[32];
		int counter = 0;
	//	Random r = new Random(so.getRandomSeed());
		Random r = new Random(123456789L);
		for (int i = 0; i < so.getDimparameter(); i++)
			rngvec[i] = (float) r.nextGaussian();
		
		so.setdim(vec.length);
	//	so.getDecoderType().setVariance(StatTests.variance(vec));
		
		// #create projector matrixs
		Projector projector = new DBFriendlyProjection();
		projector.setOrigDim(so.getdim());                      // is this working for spark ?
		projector.setProjectedDim(32);
		//projector.setRandomSeed(so.getRandomSeed());
		projector.setRandomSeed(12345678910L);
		projector.init();
		
		int ct = 0;
		if(znorm == true){
			
			float[] variance = StatTests.varianceCol(so.getRawData());
			
			
			float[] mean = StatTests.meanCols(so.getRawData());
			// #process data by adding to the counter
			for (float[] x : so.getRawData()) 
			{
				addtocounter(x, projector, IDAndCent,IDAndID,ct++,mean,variance);
			}
		}
		else
		{
			
//			for (float[] x : so.getRawData()) 
//			{
//				addtocounter(x, projector, IDAndCent, IDAndID,ct++);
//		
			while (vecs.hasNext()) {
				float[] record =  vecs.next();
				
	             addtocounter(record, projector, IDAndCent,IDAndID,ct++);            //err
				
				vec = vecs.next();
			                       }
			
			
			
			}
		
		
		
		
		return IDAndCent; }     // should it return new Object[] { IDAndCent, IDAndID } both ?
		
	
		public HashMap<Long, List<float[]>>  findDensityModesPart1() {
			HashMap<Long, List<float[]>> IDAndCent = new HashMap<>();
			HashMap<Long, List<Integer>> IDAndID = new HashMap<>();          // why is this needed ?
			// #create projector matrixs
			Projector projector = so.getProjectionType();
			projector.setOrigDim(so.getdim());
			projector.setProjectedDim(so.getDimparameter());
			//projector.setRandomSeed(so.getRandomSeed());
			projector.setRandomSeed(12345678910L);
			projector.init();
			
			int ct = 0;
			if(znorm == true){
				float[] variance = StatTests.varianceCol(so.getRawData());
				float[] mean = StatTests.meanCols(so.getRawData());
				// #process data by adding to the counter
				for (float[] x : so.getRawData()) 
				{
					addtocounter(x, projector, IDAndCent,IDAndID,ct++,mean,variance);
				}
			}
			else
			{
				
				for (float[] x : so.getRawData()) 
				{
					addtocounter(x, projector, IDAndCent, IDAndID,ct++);
				}
			}
			
			
			
			return IDAndCent; }     // should it return new Object[] { IDAndCent, IDAndID } both ?	
		
		
		
		
		
		// Here we have to write the code for merging the HashMap IDAndCent
		// break the finddensitymode func. into two : 1 returns the merged hashmap idandcent in map-reduce paradigm
		// 2 returns the list float of centroids computed in a single node.
		
			
		
		
	public static HashMap<Long, List<float[]>> mergehmapsidsandcents(HashMap<Long, List<float[]>> partidandcent1,
				HashMap<Long, List<float[]>> partidandcent2) 
	{
			
	
			{    // method to test 1.
				HashMap<Long, List<float[]>> combined  = new HashMap<Long, List<float[]>> (); // new empty map
				combined.putAll(partidandcent1);

				for(Long key : partidandcent2.keySet()) {
				    if(combined.containsKey(key)) {
				    	combined.get(key).addAll(partidandcent2.get(key));
				    } else {
				    	combined.put(key,partidandcent2.get(key));
				    }
				}
		
				return (combined);	
			}
								
	
			
	}
		
	
		
		
		// next we want to prune the tree by parent count comparison
		// follows breadthfirst search
		
				
		
		public List<List<float[]>>  findDensityModesPart2(HashMap<Long, List<float[]>> IDAndCent) {
		HashMap<Long, Long> denseSetOfIDandCount = new HashMap<Long, Long>();
		for (Long cur_id : new TreeSet<Long>(IDAndCent.keySet())) 
		{
			if (cur_id >so.getk()){
	            int cur_count = IDAndCent.get(cur_id).size();
	            long parent_id = cur_id>>>1;
	            int parent_count = IDAndCent.get(parent_id).size();
	            
	            if(cur_count!=0 && parent_count!=0)
	            {
		            if(cur_count == parent_count) {
						denseSetOfIDandCount.put(parent_id, 0L);
						IDAndCent.put(parent_id, new ArrayList<>());
						denseSetOfIDandCount.put(cur_id, (long) cur_count);
		            }
		            else
		            {
						if(2 * cur_count > parent_count) {
							denseSetOfIDandCount.remove(parent_id);
							IDAndCent.put(parent_id, new ArrayList<>());
							denseSetOfIDandCount.put(cur_id, (long) cur_count);
						}
		            }
	            }
			}
		}
		
		//remove keys with support less than 1
		Stream<Entry<Long, Long>> stream = denseSetOfIDandCount.entrySet().stream().filter(p -> p.getValue() > 1);
		//64 so 6 bits?
		//stream = stream.filter(p -> p.getKey() > 64);

		List<Long> sortedIDList= new ArrayList<>();
		// sort and limit the list
	//	stream.sorted(Entry.<Long, Long> comparingByValue().reversed()).limit(so.getk()*4)
		stream.sorted(Entry.<Long, Long> comparingByValue().reversed()).limit(so.getk()*12)
		
		       .forEachOrdered(x -> sortedIDList.add(x.getKey()));
		
		// compute centroids

		HashMap<Long, List<float[]>> estcents = new HashMap<>();
		for (int i =0; i<sortedIDList.size();i++)
		{
			estcents.put(sortedIDList.get(i), IDAndCent.get(sortedIDList.get(i)));
		}
//		System.out.println();
//		for (int i =0; i<sortedIDList.size();i++)
//		{
//			System.out.println(sortedIDList.get(i) + ":"+VectorUtil.longToString(sortedIDList.get(i))+":"+IDAndCent.get(sortedIDList.get(i)).size());
//		}
		
		
		
		return new ArrayList<>(estcents.values());}
//	}

	public void run() {
		rngvec = new float[so.getDimparameter()];
		counter = 0;
	//	Random r = new Random(so.getRandomSeed());
		Random r = new Random(123456789L);
		for (int i = 0; i < so.getDimparameter(); i++)
			rngvec[i] = (float) r.nextGaussian();
		
		HashMap<Long, List<float[]>> centsandids1 = findDensityModesPart1();
		HashMap<Long, List<float[]>> centsandids2 = findDensityModesPart1();
		HashMap<Long, List<float[]>> centsandids  =  mergehmapsidsandcents(centsandids1,centsandids2);
		
		List<List<float[]>> clustermembers = findDensityModesPart2(centsandids);
		
		List<float[]>centroids = new ArrayList<>();
		
		List<Float> weights =new ArrayList<>();
//		int k = clustermembers.size()>200+so.getk()?200+so.getk():clustermembers.size();
		
		int k = (clustermembers.size()) > (600+so.getk())  ?  (600+so.getk()) : (clustermembers.size());
		
		for(int i=0;i<k;i++){
			weights.add(new Float(clustermembers.get(i).size()));
			centroids.add(medoid(clustermembers.get(i)));
		}
		Agglomerative3 aggloOffline =  new Agglomerative3(centroids, so.getk());
		aggloOffline.setWeights(weights);
		this.centroids = aggloOffline.getCentroids();
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException {

		int k = 10;
		int d = 1000;
		int n = 10000;
		float var = 0.1f;
		int count = 10;
		System.out.printf("ClusterVar\t");
		for (int i = 0; i < count; i++)
			System.out.printf("Trial%d\t", i);
		System.out.printf("RealWCSS\n");

		for (float f = var; f < 5.01; f += .05f) {
			float avgrealwcss = 0;
			float avgtime = 0;
			System.out.printf("%f\t", f);
			for (int i = 0; i < count; i++) {
				GenerateData gen = new GenerateData(k, n / k, d, f, true, .5f);
				// gen.writeCSVToFile(new
				// File("/home/lee/Desktop/reclsh/in.csv"));
				RPHashObject o = new SimpleArrayReader(gen.data, k);
				o.setDimparameter(32);                                           // does this overwrites the default cmd line argument ?
				RPHashAdaptive2Pass rphit = new RPHashAdaptive2Pass(o);
				long startTime = System.nanoTime();
				List<Centroid> centsr = rphit.getCentroids();

				avgtime += (System.nanoTime() - startTime) / 100000000;
				
				avgrealwcss += StatTests.WCSSEFloatCentroid(gen.getMedoids(),
						gen.getData());
				
				System.out.printf("%.0f\t",
						StatTests.WCSSECentroidsFloat(centsr, gen.data));
				System.gc();
			}
			System.out.printf("%.0f\n", avgrealwcss / count);
		}
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
		this.so.setK(getk);
	}

	@Override
	public void reset(int randomseed) {
		centroids = null;
		so.setRandomSeed(randomseed);
	}

	@Override
	public boolean setMultiRun(int runs) {
		return false;
	}
}
