# MMTF Fragment Search

This program is meant to enable comprehensive querying of the PDB utilizing a fragmentation of every structure (and each chain) looking for matches based on backbone geometry - initially independent of all other properties. Under the hood the Java Apache Spark API is used in conjunction with the mmtf-spark project.

Conceptually, this allows for a strictly spatial search of fragments that utilize similar geometry to accomplish important chemistry. 

Window (fragment) size can be varied, and the __only required input is a PDB file of the query fragment__. The query fragment should be identical in length to the window size chosen.

__Example Output__
![Example output of top 1000 hits](/example.png?raw=true "Example Output")

Planned improvements:
- [ ] Output results in more parsable format (CSV)
- [ ] Make executable build
- [ ] Add sequence motif filter (regex for fragments)
- [ ] Add filter to only let a PDB ID occur once

Potential future improvements:
- [ ] GUI
- [ ] Automated checking of pre-fragmented HADOOP sequence files
- [ ] Automated updating of HADOOP sequence files with additions/changes to PDB
- [ ] Inline creation of PML script to open results in PyMOL (currently implemented with a separate python script)

Many thanks to @lafita for all his work on this project and the UCSD actively developing mmtf-spark. This software is in development and as such is provided as is, with no guarantees.
