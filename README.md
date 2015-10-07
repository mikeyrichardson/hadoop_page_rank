## PageRank with Hadoop

This project contains a python script `calculate_pagerank.py` to quickly calculate the PageRank of web pages given a graph file of tab separated node pairs (first page containing a link to the second page). It takes a single input file as arguments and has optional arguments for teleportation rate (default 0.2), convergence threshold (default 1e-4), and output file (default standard output).


It also contains a working (but not fully optimized) Hadoop version of PageRank that will work with a giant graph file. The Hadoop version breaks up the matrix and vector entries and sends them to specific reducers so that multiplication can be done block by block (and row by row). To create the jar file for running the hadoop version, type mvn install from the command line while inside the hadoop_pagerank folder (the one with the pom.xml file). This should generate a target folder that will contain `pagerank.jar`.

Here is an example of the syntax to run the program (assuming pagerank.jar is in the current directory)

```
cd hadoop_pagerank
mvn install
cd target
hadoop jar pagerank.jar net.mikeyrichardson.pagerank.CalculatePageRank \ 
  -Dmap.divs.num=2 -map.teleportation.rate=0.2 \ 
  -Dmap.epsilon.value=1e-5 input.txt output.txt
```

This will create the jar file and then use it to find the page rank of the pages in input.txt with results stored in output.txt.

There are three parameters that can be specified in the hadoop job:

* Number of divs: This controls how many different rows the matrix is broken into when sent to different reducers.
* Teleportation rate: This controls the rate at which a random page will be chosen next instead of one of the linked pages.
* Convergence threshold (epsilon): This is the threshold sum of the absolute differences between vectors after multiplying by the transition matrix. The algorithm stops when the sum drops below the threshold.