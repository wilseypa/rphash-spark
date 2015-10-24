package edu.uc.rphash_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import com.google.common.collect.Lists;
import java.util.regex.Pattern;


public class RPHashStream implements StreamClusterer {
	private float variance;
	public KHHCentroidCounter is;
	private LSH[] lshfuncs;
	private StatTests vartracker;
	private List<float[]> centroids = null;
	private RPHashObject so;

	@Override
	public synchronized long addVectorOnlineStep(float[] vec) {
		long hash[];
		Centroid c = new Centroid(vec);
		
		float tmpvar = vartracker.updateVarianceSample(vec);
		if(variance!=tmpvar){
			for (LSH lshfunc : lshfuncs) 
				lshfunc.updateDecoderVariance(tmpvar);
			variance= tmpvar;
		}
		for (LSH lshfunc : lshfuncs) 
		{
			hash = lshfunc.lshHashRadiusNo2Hash(vec, so.getNumBlur());
			for (long h : hash)
				c.addID(h);
		}
		is.add(c);
		return  is.count;
	}

	public void init() {

		Random r = new Random(so.getRandomSeed());
		this.vartracker = new StatTests(.01);
		int projections = so.getNumProjections();
		int k = (int) (so.getk() * projections);

		// initialize our counter
		float decayrate = .999f/10000f;//bottom number is window size
		
		is = new KHHCentroidCounter(k,decayrate);
		
		// create LSH Device
		lshfuncs = new LSH[projections];
		Decoder dec = so.getDecoderType();
		HashAlgorithm hal = new MurmurHash(so.getHashmod());
		// create projection matrices add to LSH Device
		for (int i = 0; i < projections; i++) {
			Projector p = new DBFriendlyProjection(so.getdim(),
					dec.getDimensionality(), r.nextLong());
			
			
			lshfuncs[i] = new LSH(dec, p, hal);
		}
		
	}
	
	public RPHashStream(int k, ClusterGenerator c) {

		so = new SimpleArrayReader(k,c);
		init();
	}

	public RPHashStream(List<float[]> data, int k) {

		variance = StatTests.varianceSample(data, .01f);
		so = new SimpleArrayReader(data, k);
		init();
	}

	public RPHashStream(RPHashObject so) {
		this.so = so;
		init();
	}

	public List<float[]> getCentroids(RPHashObject so) {
		this.so = so;
		init();
		if (centroids == null)
			run();
		centroids = new ArrayList<float[]>();
		for (Centroid c : is.getTop())
			centroids.add(c.centroid());
		return new Kmeans(so.getk(), centroids, is.getCounts()).getCentroids();
	}

	@Override
	public List<float[]> getCentroids() 
	{
		if (centroids == null){
			run();
			centroids = new ArrayList<float[]>();
			for (Centroid cent : is.getTop())
				centroids.add(cent.centroid());
			centroids = new Kmeans(so.getk(), centroids, is.getCounts()).getCentroids();
		}
		return centroids;
	}
	
	ArrayList<Float> counts;
	public List<float[]> getCentroidsOfflineStep() 
	{
		
		//if(centroids == null ){
			centroids = new ArrayList<float[]>();
			counts = new ArrayList<Float>();
		//}
		
		for (int i = 0 ;i<is.getTop().size();i++){
			centroids.add(is.getTop().get(i).centroid());
			counts.add(is.getCounts().get(i));
		}

		centroids = new Kmeans(so.getk(), centroids,counts).getCentroids();
		
		int count = (int) ((Collections.max(counts)+Collections.min(counts))/2);
		counts = new ArrayList<Float>();
		for(int i = 0;i<so.getk();i++)counts.add((float) count);
		return centroids;
	}

	public void run() {
		// add to frequent itemset the hashed Decoded randomly projected vector
		Iterator<float[]> vecs = so.getVectorIterator();
		while (vecs.hasNext()) {
			addVectorOnlineStep(vecs.next());
		}
	}

	public List<Float> getTopIdSizes() {
		return is.getCounts();
	}

	public static void main(String[] args) throws Exception{

		int k = 10;
		int d = 100;
		int n = 20000;
		float var = .75f;
		// Create a local StreamingContext with four working threads and batch interval of 2 second
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("StreamingRPHash_Spark")
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2))
		JavaReceiverInputDStream<List> vectors = jssc.socketTextStream("localhost", 9999)
		RPHashStream rphit = new RPHashStream(vectors, k);
		long startTime = System.nanoTime();
				rphit.getCentroids();
				long duration = (System.nanoTime() - startTime);
				
				List<float[]> aligned = TestUtil.alignCentroids(
						rphit.getCentroids(), gen.medoids());
				System.out.println(f + ":" + StatTests.PR(aligned, gen) + ":"
						+ StatTests.WCSSE(gen.medoids(), gen.getData()) + ":"
						+ StatTests.WCSSE(aligned, gen.getData()) + ":" + duration
						/ 1000000000f);
				System.gc();
			}
		}

		Runtime rt = Runtime.getRuntime();
		GenerateStreamData gen = new GenerateStreamData(k, d, var,25l);
		StreamClusterer rphit = new /*StreamingKmeans(k,gen);*/RPHashStream(k,gen);
		
		ArrayList<float[]> vecsInThisRound = new ArrayList<float[]> ();
		System.out.printf("Vecs\tMem(KB)\tTime\tWCSSE\tPR\tCentSSE\n");
		long gentime = System.nanoTime();

		for (int i = 0; i < 10000; i++) {
			float[] f = gen.generateNext();
			vecsInThisRound.add(f);
			if (i % 10000 == 10000-1) 
			{
				gentime = System.nanoTime()-gentime;			
				vecsInThisRound = new ArrayList<float[]> ();
			}
		}
		
		long timestart = System.nanoTime();
		for (int i = 0; i < 1000000; i++) {
			float[] f = gen.generateNext();
			rphit.addVectorOnlineStep(f);
			vecsInThisRound.add(f);
			if (i % 10000 == 10000-1) 
			{
				List<float[]> cents = rphit.getCentroidsOfflineStep();
				long time = System.nanoTime() - timestart;
				double wcsse = StatTests.WCSSE(cents, vecsInThisRound);
				vecsInThisRound = new ArrayList<float[]> ();
				rt.gc();
				Thread.sleep(10);
				rt.gc();
				

				long usedkB = (rt.totalMemory() - rt.freeMemory()) / 1024;
				List<float[]> aligned = TestUtil.alignCentroids(
				cents, gen.getMedoids());
				double pr = StatTests.PR(aligned, gen);
//				double intercluster = StatTests.WCSSE(aligned, gen.getData());
				double ssecent = StatTests.SSE(aligned, gen);

				System.out.printf("%d\t%d\t%.3f\t%.0f\t%.3f\t%.3f\n",i,usedkB, (time-gentime)/1000000000f,wcsse,pr,ssecent);
				timestart = System.nanoTime();
			}
		}
		jssc.start();
		jssc.awaitTermination();
	}

	@Override
	public RPHashObject getParam() {
		return so;
	}

}
