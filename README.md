# Process and extract data from IMDB datasets 

This project coniains code fo rprocessing imdb datasets.

In order to get expected results the followin datasets were used:

name.basics.tsv
title.akas.csv
title.principals.tsv
title.ratings.tsv


## Dependencies

Need to have following installed on a linux machine(tested on Ubnutu 20)

Oracle Java 8
Maven 3
Spark 2.4.7

## How to run the code

1. build and package the code
	- change dir to foder containing pom.xml file
	- run mvn clean install

2. go to /target folder created by maven
        - make sure to export $JAVA_HOME
	- make sure $SPARK_HOME/bin is in your $PATH
	- run the code with following command:

	spark-submit --class dataset.ImdbProcessor ./dataset-processor-1.0-SNAPSHOT.jar /path/to/dataset/folder

3. see the results
	- the results will be stored in current /target subfolders: 

	 actors.csv/partXXX.csv file
	 aka_names.csv/partYYY.csv file
	 
4. configurations
	src/main/java/utils/Configurations.java 
	holds processing configurations such as rating threshold(default 500) and top movies to analyze(default 10)	
