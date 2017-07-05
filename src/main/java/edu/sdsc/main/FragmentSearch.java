package edu.sdsc.main;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.GroupType;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;

import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.ContainsSequenceRegex;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * Search similar fragments to a query in the PDB.
 * 
 * @author Sri
 * @author Kyle
 * @author Aleix
 *
 */
public class FragmentSearch {

	public static void main( String[] args ) throws IOException, StructureException {
		//Add counter for printing performance metrics
		long start = System.nanoTime();
    	// Quick hack, the user has to take care of providing that

    	AminoAcidImpl[] query = (AminoAcidImpl[]) StructureIO.getStructure("~/Downloads/5epc_fragment.pdb")
    			.getChainByIndex(0).getAtomGroups(GroupType.AMINOACID).toArray(new AminoAcidImpl[5]);
    	
    	
    	String path = System.getProperty("MMTF_FULL");
		if (path == null) {
			System.err.println("Path for full Hadoop sequence file has not been set");
			System.exit(-1);
		}

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FragmentSearch.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);


		JavaRDD<Row> rows = MmtfReader
				//for searching only 1, or a subset of structures 
				//.downloadMmtfFiles(Arrays.asList("5jn5"), sc)
				//.downloadMmtfFiles(Arrays.asList("5epc","5jn5", "5tr2", "5f9c", "5hsh", "2dhd", "1din","2ack","1thg","1tht","1zrs","1gpm","3pmg"), sc)
				.readSequenceFile(path, sc)
				.filter(new ContainsLProteinChain()) // at least 1 protein chain required
				.flatMapToPair(new StructureToPolymerChains()) // split into polymer chains
				.filter(new ContainsLProteinChain()) // make sure this chain is a protein
				.mapValues(new StructureToBioJava()) // convert to a BioJava structure
				.flatMapToPair(new BioJavaStructureToFragments(query.length)) //fragment all of the chains remaining
				.filter(new ConsecutiveFragment())
				//.filter(new ContainsSequenceRegex("[TSG].{1}S.{1}[GNP]."))
				.mapToPair(new SimilarityScorer(query))
				.map(new ResultsDataset());
		
		//Optionally filter based on regex sequence as well ? ( GXSXG ish motif) 
		//.filter(new ContainsSequenceRegex("[TSG].{1}S.{1}[GNP].")); from solution 4 basic-spark
		
		Dataset<Row> ds = JavaRDDToDataset.getDataset(rows, "pdb","chain","resnum","score");
		ds.sort(col("score").asc()).show(1000);
		long end = System.nanoTime();
	    long count = MmtfReader.readSequenceFile(path, sc).count();    
		printMetrics(count, start, end);
		sc.close();
    }

	private static void printMetrics(long count, long start, long end) {
		int cores = Runtime.getRuntime().availableProcessors();
		String osName = System.getProperty("os.name");
	    String osType= System.getProperty("os.arch");
	    String osVersion= System.getProperty("os.version");
	    String time = String.format("%.1f", (end-start)/1E9);
	    System.out.println("\nIt took " + time + " seconds to process " + count + " PDB files on " 
	    + cores + " cores, using " + osName + "-" + osType + ", Version " + osVersion + ". That's impressive.");
	}
}
