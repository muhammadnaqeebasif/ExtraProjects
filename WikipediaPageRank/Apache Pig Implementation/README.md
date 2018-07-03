# Wikipedia Page Rank
In this project the data is exported from wikipedia.org. The xml file is loaded using custom Pig LoadFunc 'PageRankLoad' which first reads the file using custom InputFormat 'XmlInputFormat' which gives the records such that each record is a page i.e. enclosed in page tag. PageRankLoad extracts the required data from the records such that it outputs a tuple containing the title of the page and the bag of links out of the page. After the data is loaded page rank is calculated using Apache Pig. Following are the files and folder:

* ```PigPageLoad``` Eclipse project to create 'XmlInputFormat' and 'PageRankLoad'. 
* ```pigPageLoad.jar``` The jar file of the above mentioned eclipse project.
* ```pagerank.pig``` The pig script file for computing the pagerank.
* ```input.xml``` The input used in the project 
* ```Output``` The folder containing the result generated.

## Note
The project is run on ```Cloudera quickstart vm```.