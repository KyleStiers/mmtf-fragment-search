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
				//.downloadMmtfFiles(Arrays.asList("5jn5"), sc)
				//.downloadMmtfFiles(Arrays.asList("5jn5", "5tr2", "5f9c", "5hsh"), sc)
				.readSequenceFile(path, sc)
				.filter(new ContainsLProteinChain()) // at least 1 protein chain required
				.flatMapToPair(new StructureToPolymerChains()) // split into polymer chains
				.filter(new ContainsLProteinChain()) // make sure this chain is a protein
				.mapValues(new StructureToBioJava()) // convert to a BioJava structure
				.flatMapToPair(new BioJavaStructureToFragments(query.length))
				.filter(new ConsecutiveFragment())
				.mapToPair(new SimilariryScorer(query))
				.map(new ResultsDataset());
		
		Dataset<Row> ds = JavaRDDToDataset.getDataset(rows, "pdb","chain","resnum","score");
		
		ds.sort(col("score").asc()).show(100);
		
		sc.close();
		
    }
}
