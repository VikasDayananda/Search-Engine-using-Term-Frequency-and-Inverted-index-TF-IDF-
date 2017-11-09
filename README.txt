Information retrieval using TF-IDF values.
By Vikas Dayananda, vdayanan@uncc.edu, 800969865

Compiler and Platform
Compiler : Java compiler (javac)
Java version : JDK 1.8.0
Programming : Java
Platform : Hadoop / Cloudera.
--------------------------------------------------------------------------------------------------------------------------------------
Project Files:
1. DocWordCount.java
This program counts occurrences of the words in each of the input files. The
output will be in format: “word#####filename count”
2. TermFrequency.java
This program calculates the Term Frequency value for each word in the file
using WF(t,d) = 1+ log10 (TF(t,d) .
The output will be in format : “word#####filename value”
3. TFIDF.java
This program calculates TF-IDF values for each word in the file using
TF-IDF(t,d) = WF(t,d)* IDF(t)
Where IDF(t) = log10 (1+Toatl no doc / no of docs containing word).
The output will be in format: “word#####filename score”
4. Search.java
This program accepts user query and outputs list of documents with scores
that matches the query. The output will be in format: “filename score”
---------------------------------------------------------------------------------------------------------
Execution:

Preparing Input and Output.
1. Before you run the program, you must create you must create input and output locations in
HDFS. Open terminal and follow to steps to create directory in HDFSi. sudo su hdfs
ii. hadoop fs -mkdir /user/<username>
iii. hadoop fs -chown <username> /user/<username>
iv. exit
v. sudo su <username>
vi. hadoop fs -mkdir /user/<username>/tfidf  /user/<username>/tfidf/input
2. Move CANTERBURY files to input directory in HDFS
i. hadoop fs -put <Canterbury-files-path> /user/<username>/tfidf/input

--------------------------------------------------------------------------------------------------------------------------------------
Execution of DocWordCount.java ( Two arguments)
1. Compile the file.
i. mkdir –p build
ii. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*
DocWordCount.java -d build –Xlint
2. Create jar file
i. jar -cvf docwordcount.jar -C build/ .
3. Run the jar file by passing input and output locations as arguments.
i. $ hadoop jar docwordcount.jar org.vikas.DocWordCount
/user/<username>/tfidf/input /user/<username>/tfidf/output
4. See output files
hadoop fs -cat /user/<username>/tfidf/output/*

--------------------------------------------------------------------------------------------------------------------------------------
Execution of TermFrequency.java ( Two arguments)
1. Compile the file.
i. mkdir –p build
ii. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*
TermFrequency.java -d build –Xlint
2. Create jar file
i. jar -cvf TermFrequency.jar -C build/ .
3. Run the jar file by passing input and output locations as arguments.
i. $ hadoop jar TermFrequency.jar org.vikas.TermFrequency
/user/<username>/tfidf/input /user/<username>/tfidf/output_tf
4. See output files
hadoop fs -cat /user/<username>/tfidf/output_tf/*

--------------------------------------------------------------------------------------------------------------------------------------
Execution of TFIDF.java ( Three arguments)
1. Compile the file. You should compile both TermFrequency and TFIDF files. Hence use T*.java
i. mkdir –p build
ii. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* T*.java -d
build –Xlint
2. Create jar file
i. jar -cvf TFIDF.jar -C build/ .
3. Run the jar file by passing input and output locations as arguments. There are three
arguments. Intermediate output path should be given.
i. $ hadoop jar TFIDF.jar org.vikas.TFIDF /user/<username>/tfidf/input
/user/<username>/tfidf/output_temp /user/<username>/tfidf/output_tfidf
4. See output files
hadoop fs -cat /user/<username>/tfidf/output_tfidf/*

--------------------------------------------------------------------------------------------------------------------------------------
Execution of Search.java ( Two arguments plus user query)
1. Compile the file.
i. mkdir –p build
ii. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d
build –Xlint
2. Create jar file
i. jar -cvf Search.jar -C build/ .
3. Run the jar file by passing input and output locations as arguments. Output of the TFIDF is
the input argument of this program. User query argument should not contain any quotes.
i. $ hadoop jar Search.jar org.vikas.Search /user/<username>/tfidf/output_tfidf /user/<username>/tfidf/output_search user query
4. See output files
hadoop fs -cat /user/<username>/tfidf/output_search /*
--------------------------------------------------------------------------------------------------------------------------------------
NOTE: The words are considered to be case-insensitive, i.e. the words “RUN” and “run” is considered
same and counted. The execution steps are given in references to running program in packages
installation of CDH in Cloudera.

By,
Vikas Dayananda
vdayanan@uncc.edu
800969865