# Hadoop Test Driver Manual
==================

Terasort and TestDFSIO test driver

Version 0.5

Claudio Fahey (claudio.fahey@emc.com)

# Overview

This Hadoop Test Driver runs DFS I/O, Teragen, and Terasort benchmarks with a variety of parameters in a completely unattended manner. The results will be record to a JSON file allowing them to be analyzed by [test-result-analyzer](https://github.com/claudiofahey/test-result-analyzer).

An optional custom DFS IO test is a modified version of TestDFSIO that forces all I/O to be performed simultaneously, avoiding skew caused by slow running tasks.

# Prerequisites

1. A Hadoop cluster running one of the following versions:
  1. Pivotal PHD 1.1
  2. Cloudera CDH 4.5

2. A Linux server that will be used to run the test driver. This server must be able to SSH into the following:
  1. A server with the Hadoop client
  2. An Isilon node

3. Password-less SSH access must be enabled for the above connections. See http://www.tecmint.com/ssh-passwordless-login-using-ssh-keygen-in-5-easy-steps/.
4. Times on all compute nodes must be synchronized to within a second, preferably using NTP.

# Installation

1. Extract hadoop-test-driver.tar.gz to your Linux server.
2. Copy the file com.emc.hadoop-tests-0.3.jar to the gpadmin home directory of your Hadoop client node.
3. Copy site.cfg to mysite.cfg and edit mysite.cfg with the desired parameters that describe your site.
4. Edit kill-all-jobs.sh. Specify the correct Hadoop client user and host.

# Running DFS IO Tests

1. To define the tests to run, edit the file generate-dfsio-tests.pl as desired. See the Test Parameters section for details.
2. Run the above script to save the test definitions to a file.$ ./generate- dfsio -tests.pl > dfsio.tests
3. Run the test driver with the site configuration and the test definitions:$ ./hadoop-test-driver.pl mysite.cfg dfsio.tests

# Running Terasort Tests

1. To define the tests to run, edit the file generate-terasort-tests.pl as desired. See the Test Parameters section for details.
2. Run the above script to save the test definitions to a file.$ ./generate-terasort-tests.pl > terasort.tests
3. Run the test driver with the site configuration and the test definitions:$ ./hadoop-test-driver.pl mysite.cfg terasort.tests

# Test Parameters

| Parameter Name | Description |
| --- | --- |
| Common Parameters |
| baseDirectory | The HDFS directory that will be used to create temporary files for benchmarking. |
| blockSizeMiB | Sets the Isilon HDFS block size used for reading files. |
| flushIsilon | If 1, the Isilon cache is flushed prior to the test.   
WARNING: This should not be enabled on production systems! |
| hadoopClientHost | IP or DNS name of the server with the Hadoop client. |
| hadoopClientUser | User to SSH into the Hadoop client as. |
| hadoopParameters | Other parameters to pass to the Hadoop command line. This should specify the memory requirements. |
| hdfsThreads | Before starting the test, the Isilon HDFS daemon will be configured to use this many threads. |
| ioFileBufferSize | Corresponds to the Hadoop parameter io.file.buffer.size. |
| isiHost | Isilon host IP or DNS name. |
| isiUser | User to SSH into Isilon as. |
| jar | The Java JAR file to pass to the Hadoop client. |
| numComputeNodes | The number of compute nodes currently active. Currently this is not used except when recording the results. |
| numIsilonNodes | The number of Isilon nodes currently active. Currently this is not used except when recording the results. |
| resultCsvFilename | Test results will be append to this file. |
| shuffleDirectory | not used |
| shufflePlugIn | must be "default" |
| tag | A user-defined identifier for the test. The generate\*.pl scripts use an incrementing number. |
| test | The type of test to run. Available values are write, read, teragen, terasort, command |
| DFS IO Parameters (write, read) |
| bufferSize | The buffer size used by TestDFSIO. |
| nrFiles | The number of files to use simultaneously. This equals the number of threads. You must have enough map slots to allow each map task to run concurrently. The job will ensure that each task runs at exactly the right time or it throws an exception if unable to do so. |
| startIOSec | I/O will begin this many seconds after the job is submitted. All tasks must have started by this time or the job will fail. |
| startMeasurementSec | Measurement of I/O speed will begin this many seconds after I/O begins. |
| stopAfterSec | I/O will stop this many seconds after measurement begins. |
| Teragen Parameters |
| dataSizeMB | The total size of all files generated. |
| mapTasks | The number of map tasks that will create the files. This will equal the number of files. |
| Terasort Parameters |
| mapOutputCompressCodec | Set the value of the Hadoop parameter mapred.map.output.compress.codec. |
| reduceTasks | The number of reduce tasks. |
| sortFactor | Set the value of the Hadoop parameter io.sort.factor. |
| sortMiB | Set the value of the Hadoop parameter mapreduce.task.io.sort.mb. |
