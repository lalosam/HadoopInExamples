# Hadoop in examples

This repo include several versions of the WordCount MapReduce job using different strategies/features to exemplify their use.

The idea is to keep things simple and only add a one new thing in each example. The jobs wasn't designed to be optimals and obviously there are jobs that perform better that others. Not all the "configurations" could be the best working with the same challenge.

The project include a main class that generate diferent input scenarios, like one big file (500 MB in one file) or a lot of small files (500 MB in 1000 files). The files are generated in the default user file in Hadoop, "/user/<username>" by default. The class is: https://github.com/lalosam/HadoopInExamples/blob/master/src/main/java/rojosam/process/tests/GenerateTestFiles.java

**WARNING:** Validate that the current paths don't have conflict with your current data.

By now only the TextFiles are being used in the MapReduce jobs. The text files are being generated using the Linux English dictionary (included in the resources) picking random words.

Since this project is designed for **developers** learning MapReduce , feel free to go into the code and make your own adjustments if it is necessary.

To build the jar before execute the maps:

```
mvn package
```

To generate the test files:

```
mvn exec:java -Dexec.mainClass="rojosam.process.tests.GenerateTestFiles" -Dexec.daemonThreadJoinTimeout=90000
```
If you get the exception:

```
java.lang.IllegalThreadStateException
```
please increase the timeout parameter to avoid Maven kill the execution if it takes too long.

To execute the MapReduce jobs:

```
hadoop jar HadoopInExamples-1.0.jar rojosam.hadoop.CombinedInputWordCount.DriverCK wordfiles customkey
```

Change the driver class or the input and output paths as required.


## Common questions

A lot of peopleis asking in the internet about the impact in the performance working with small files and big files. Well, this project could help you to understand it.

The big file is designed to have 4 hadoop blocks (based on default 128MB).

Then you can execute the **SimpleWordCount** job and validate how many maps are created and how many nodes are working in parallel. And if you run it in a standalone or in a pseudo cluster you could monitor the use of the memory and cores.

Then, try the same job with multiple small files and compare the performance (time, memory, CPU, etc.)

After that, you can execute the **CombinedInputWordCount** to see the diference when you combine several inputs in one Mapper and compare the time against a one big file and with multiple small files. Notice that in both cases you are procesing almost the same amount of data (~500MB).




