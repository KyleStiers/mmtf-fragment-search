# Fragment Searching across the PDB using MMTF with **mmtf-fragment-search**
_A way of querying all fragments in the PDB (~32 million) for a specific property implementing the recently developed mmtf-spark library._

### BioJavaStructureToFragments class 
Provides functionality for fragmenting the entire PDB based upon an arbitrary window size.

### ConsecutiveFragment class
Filters out non-consecutive peptides using Calc.isConnected from BioJava

### SimilarityScorer class
Calculates a very simple metric for assessing the similarity of a query fragment to the target fragment. It is calculated as the sum of differences in angles at each position in the window. *(Score of 0 is perfect)*

### ResultsDataset class
Converts the JavaRDD to a Spark Dataset for displaying and added functionality of sorting/fetching columns

### FragmentSearch class
Main class that puts everything together, keeps performance metrics, and prints the desired number of sorted (ascending) fragment hits.

