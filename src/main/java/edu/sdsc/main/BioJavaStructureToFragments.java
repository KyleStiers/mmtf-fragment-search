package edu.sdsc.main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.GroupType;
import org.biojava.nbio.structure.Structure;
import scala.Tuple2;

/**
 * Gets a protein chain and returns the fragments of window size.
 * 
 * @author Sri
 * @author Kyle
 * @author Aleix
 * 
 */
public class BioJavaStructureToFragments implements PairFlatMapFunction<Tuple2<String,Structure>, String, Group[]> {
	private static final long serialVersionUID = -1234631110714432408L;

	private int window = 5;
	
	public BioJavaStructureToFragments(int window) {
		this.window = window;
	};
	
	public BioJavaStructureToFragments() {
		this.window = 5;
	};
	
	public Iterator<Tuple2<String, Group[]>> call(Tuple2<String, Structure> t) throws Exception {
		
		List<Tuple2<String, Group[]>> fragments = new ArrayList<Tuple2<String, Group[]>>();
		
		String chainid = t._1;
		Structure s = t._2;
		
		for (int i = 0; i < s.getChainByIndex(0).getAtomGroups(GroupType.AMINOACID).size() - window; i++) {
			
			String id = chainid + "_" + s.getChainByIndex(0).getAtomGroups(GroupType.AMINOACID).get(i).getResidueNumber().toString();
					
			Group[] fragment = new Group[window];
			
			for (int j = 0; j < window; j++) {
				fragment[j] = s.getChainByIndex(0).getAtomGroups(GroupType.AMINOACID).get(i + j);
			}
			
			Tuple2<String, Group[]> f = new Tuple2<String, Group[]>(id, fragment);
			
			fragments.add(f);
			
		}
		
		
		return fragments.iterator();
	}
}
