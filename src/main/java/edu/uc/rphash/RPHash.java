package edu.uc.rphash;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.uc.rphash.tests.clusterers.Agglomerative3;
import edu.uc.rphash.util.VectorUtil;

//import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;



public class RPHash {


	public static void main(String[] args) throws NumberFormatException,
			IOException, InterruptedException {

		if (args.length < 3) {
			System.out
					.print("Usage: rphash InputFile k OutputFile [CLUSTERING_METHOD ...][OPTIONAL_ARG=value ...]\n");
		
			System.exit(0);
		}
	
//		List<float[]> data = null;	
		
//		String filename = args[0];
		
		final String filename =  "/work/deysn/rphash/data/data500.mat";
		int k = Integer.parseInt(args[1]);
		String outputFile = args[2];

		boolean raw = false;
		  
		if (args.length == 3) {	
			SparkConf conf = new SparkConf().setAppName("RPHashSimple_Spark");
		
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			
			//make a dummy list of integers for each compute node
		    int slices =  3 ;                                              //number of compute nodes or machines
		    int n = slices;
		    List<Object> l = new ArrayList<>(n);
		    for (int i = 0; i < n; i++) {
		      l.add(i);
		    }

		    JavaRDD<Object> dataSet = sc.parallelize(l);

		    List<Long>[] topids = dataSet.map(new Function<Object, List<Long>[]>()   // this pass returns the merged topids list
		    {
				private static final long serialVersionUID = -7127935862696405148L;
				
				@Override
			      public List<Long>[] call(Object integer) {
			        return RPHashSimple.mapphase1(k,filename);
			      }
				
		    }).reduce(new Function2<List<Long>[], List<Long>[], List<Long>[]>() {
				private static final long serialVersionUID = 4294461355112957651L;
				
		
				@Override
				public List<Long>[] call(List<Long>[] topidsandcounts1, List<Long>[] topidsandcounts2) throws Exception {

					return RPHashSimple.reducephase1(topidsandcounts1,topidsandcounts2);
				}
			    });	
				
	// now we need to propagate the list of topids to all machines . how ? should we explicitly do it through the rdd ?
		    
		    Object[] centroids = dataSet.map(new Function<Object, Object[]>()      // this returns the merged list of centroids
    	    {
				private static final long serialVersionUID = 1L;

			@Override
    	      public Object[] call(Object o) {
    	        return RPHashSimple.mapphase2(topids,filename);                   // does this propagate the topids to all machines ?
    	      }
    	    }).
    	    reduce(new Function2<Object[], Object[], Object[]>() {

			private static final long serialVersionUID = 1L;

			@Override
    		public Object[] call(Object[] cents1, Object[] cents2) throws Exception {
    			return RPHashSimple.reducephase2(cents1,cents2);
    		}
    	    });
		    
		  
		  //offline cluster
		    VectorUtil.writeCentroidsToFile(new File(outputFile + ".mat"),new Agglomerative3((List)centroids[0], (k)).getCentroids(), raw);     // changed the last argument from false, to raw
		    
		    
		    sc.close();	
		    sc.stop();
		    
		    }
		
		
/*		if (args.length == 4) {	
			SparkConf conf = new SparkConf().setAppName("RPHashAdaptive2Pass_Spark");
		
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			
			//make a dummy list of integers for each compute node
		    int slices =  3 ;                                              //number of compute nodes or machines
		    int n = slices;
		    List<Object> l = new ArrayList<>(n);
		    for (int i = 0; i < n; i++) {
		      l.add(i);
		    }

		    JavaRDD<Object> dataSet = sc.parallelize(l);                  // distributes the null RDD to the nodes/workers

		    List<Long>[] topids = dataSet.map(new Function<Object, List<Long>[]>()   // this pass returns the merged topids list
		    {
				private static final long serialVersionUID = -7127935862696405148L;
				
				@Override
			      public List<Long>[] call(Object integer) {
			        return RPHashSimple.mapphase1(k,filename);
			      }
				
		    }).reduce(new Function2<List<Long>[], List<Long>[], List<Long>[]>() {
				private static final long serialVersionUID = 4294461355112957651L;
				
		
				@Override
				public List<Long>[] call(List<Long>[] topidsandcounts1, List<Long>[] topidsandcounts2) throws Exception {

					return RPHashSimple.reducephase1(topidsandcounts1,topidsandcounts2);
				}
			    });	
				
		    
		    
		    
	// now we need to propagate the list of topids to all machines . how ? should we explicitly do it through the rdd ?
		    
		   
		    
		  
		  //offline cluster
	//	    VectorUtil.writeCentroidsToFile(new File(outputFile + ".mat"),new Agglomerative3((List)centroids[0], k).getCentroids(), raw);     // changed the last argument from false, to raw
		    
		    
		    sc.close();	
		    sc.stop();
		    
		    }	*/	
		
		
		
		}
		
	
}
