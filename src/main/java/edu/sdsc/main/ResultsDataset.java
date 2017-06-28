package edu.sdsc.main;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

/**
 *
 */
public class ResultsDataset implements  Function<Tuple2<String, Double>, Row> {
	private static final long serialVersionUID = -3348372120358649240L;
    
	@Override
	public Row call(Tuple2<String, Double> t) throws Exception {
		
		String id = t._1;
		Double score = t._2;
		
		String pdb = id.split("\\.")[0];
		String chain = id.split("\\.")[1].split("_")[0];
		String index = id.split("_")[1];
		
		Row row = RowFactory.create(pdb, chain, index, score);
		
		return row;
	}

}