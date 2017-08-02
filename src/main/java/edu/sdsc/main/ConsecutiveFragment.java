package edu.sdsc.main;

import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.AminoAcid;
import org.biojava.nbio.structure.Calc;
import org.biojava.nbio.structure.Group;
import scala.Tuple2;

public class ConsecutiveFragment implements Function<Tuple2<String, Group[]>, Boolean> {

	private static final long serialVersionUID = -1453951435828316728L;

	@Override
	public Boolean call(Tuple2<String, Group[]> t) throws Exception {
		
		Group[] fragment = t._2;
		
		for (int i = 0; i < fragment.length - 1; i++) {
			
			if (! Calc.isConnected((AminoAcid) fragment[i], (AminoAcid) fragment[i+1]))
				return false;
			
			//potentially could check regex here?
		}
		
		return true;
	}

}
