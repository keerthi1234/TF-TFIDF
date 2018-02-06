NAME :KEERTHI SAGAR BUKKAPURAM
UNCC ID :800954091

compilation steps for the execution of the DocWordCount, TermFrequency, TFIDF, Search java files are as follows:

Given all the input text files are included in the cantrbry folder

All the files present in the cantrbry folder are given to the DocWordCount.java file as the input

Following commands are used to run the programs in terminal:

Commands for execution of DocWordCount java program :
->$ mkdir -p build
->$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint
->$ jar -cvf wordcount.jar -C build/ .
->$ hadoop fs -rm -r -f /user/cloudera/wordcount/output
->$ hadoop jar wordcount.jar org.myorg.DocWordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output
->$ hadoop fs -cat /user/cloudera/wordcount/output/* 

Commands for execution of TermFrequency java program :
 $ mkdir -p build
 $ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint
 $ jar -cvf wordcount.jar -C build/ .
 $ hadoop fs -rm -r -f /user/cloudera/wordcount/output
 $ hadoop jar wordcount.jar org.myorg.TermFrequency /user/cloudera/wordcount/input /user/cloudera/wordcount/output
 $ hadoop fs -cat /user/cloudera/wordcount/output/*

Commands for execution of TFIDF program :
 $ mkdir -p build
 $ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build -Xlint
 $ jar -cvf wordcount.jar -C build/ .
 $ hadoop fs -rm -r -f /user/cloudera/wordcount/output
 $ hadoop jar wordcount.jar org.myorg.TFIDF /user/cloudera/wordcount/input /user/cloudera/wordcount/output
 $ hadoop fs -cat /user/cloudera/wordcount/output/* 

Commands for execution of search java program :
$ mkdir -p build
 $ hadoop fs -put TFIDF.out /user/cloudera/wordcount/input
 $ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* search.java -d build -Xlint
 $ jar -cvf wordcount.jar -C build/ .
 $ hadoop fs -rm -r -f /user/cloudera/wordcount/output
 $ hadoop jar wordcount.jar org.myorg.search /user/cloudera/wordcount/input/TFIDF.out /user/cloudera/wordcount/output "Q U E R Y"
 $ hadoop fs -cat /user/cloudera/wordcount/output/* 


Execution steps to run in eclipse:

1. create a package and create  all the java files 
2. right click on the java file 
3. click on run as configuration
4. click on the arguments 
5  Give the inputpath and outputpath
6  click on apply
7  click on run
8  Same steps used for the execution of all the java programs
9  Input and Output paths are given according to the java file being executed. 