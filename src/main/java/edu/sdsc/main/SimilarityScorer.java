package edu.sdsc.main;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.AminoAcid;
import org.biojava.nbio.structure.Calc;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.StructureException;

import scala.Tuple2;

public class SimilarityScorer implements PairFunction<Tuple2<String,Group[]>,String, Double> {
	
	private static final long serialVersionUID = -2512695129516203908L;
	
	private AminoAcid[] query;
	
	public SimilarityScorer(AminoAcid[] query) throws StructureException {
		this.query = query;
	}

	@Override
	public Tuple2<String, Double> call(Tuple2<String, Group[]> t) throws Exception {
		
		double score = 0;
		
		Group[] fragment = t._2;
		
		try {
			
			for (int i = 0; i < fragment.length - 1; i++) {
				
				double phi_f = Math.toRadians(Calc.getPhi((AminoAcid) fragment[i], (AminoAcid) fragment[i + 1]));
				double psi_f = Math.toRadians(Calc.getPsi((AminoAcid) fragment[i], (AminoAcid) fragment[i + 1]));
				
				double phi_q = Math.toRadians(Calc.getPhi(query[i], query[i + 1]));
				double psi_q = Math.toRadians(Calc.getPsi(query[i], query[i + 1]));
				
				double phi_d = Math.abs(Math.sin(phi_f) - Math.sin(phi_q));
				double psi_d = Math.abs(Math.sin(psi_f) - Math.sin(psi_q));
				
				score += phi_d + psi_d;
			}
			
			score /= (fragment.length * 2);
			
		} catch (Exception e) {
			score = 1;
		}
				
		return new Tuple2<String, Double>(t._1, score);
	}

}
