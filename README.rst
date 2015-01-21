
Hadoop Test Driver Manual
=========================

Terasort and TestDFSIO test driver

Claudio Fahey (claudio.fahey@emc.com)

********
Overview
********

This Hadoop Test Driver runs DFS I/O, Teragen, and Terasort benchmarks with a variety of parameters in a completely unattended manner.
The results will be record to a JSON file allowing them to be analyzed by test-result-analyzer available from
https://github.com/claudiofahey/test-result-analyzer.

Although this test driver has specific support for EMC Isilon as an HDFS storage platform, it can be used in a non-Isilon environment by not specifying any Isilon-related parameters.   

*************
Prerequisites
*************

1. A Hadoop cluster running one of the following versions:

   1. Pivotal PHD 2.1.0
   2. Cloudera CDH 5.1.3
   3. Hortonworks HDP 2.1

2. A Linux server that will be used to run the test driver. This server must be able to SSH into the following:

   1. A server with the Hadoop client
   2. An Isilon node

3. Password-less SSH access must be enabled for the above connections. See http://www.tecmint.com/ssh-passwordless-login-using-ssh-keygen-in-5-easy-steps/.

4. Times on all compute nodes must be synchronized to within a second, preferably using NTP.   

************
Installation
************

1. Extract hadoop-test-driver.tar.gz to your Linux server.

2. Copy site.cfg to mysite.cfg and edit mysite.cfg with the desired parameters that describe your site.

********************
Running DFS IO Tests
********************

1. To define the tests to run, edit the file generate-dfsio-tests.pl as desired. See the Test Parameters section for details.

2. Run the above script to save the test definitions to a file.

   $ ./generate- dfsio -tests.pl > dfsio.tests

3. Run the test driver with the site configuration and the test definitions:

   $ ./hadoop-test-driver.pl mysite.cfg dfsio.tests

**********************
Running Terasort Tests
**********************

1. To define the tests to run, edit the file generate-terasort-tests.pl as desired. See the Test Parameters section for details.

2. Run the above script to save the test definitions to a file.

   $ ./generate-terasort-tests.pl > terasort.tests

3. Run the test driver with the site configuration and the test definitions:

   $ ./hadoop-test-driver.pl mysite.cfg terasort.tests

***************
Test Parameters
***************

Parameters not listed below can be specified and they will pass through to the result file.
This will allow you to record other environmental parameters.

+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| Parameter Name                    | Description                                                                                             |
+===================================+=========================================================================================================+
| **Common Parameters*              |                                                                                                         |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| appMasterMemoryMB                 | Memory to allocate to the Application Master.                                                           |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| baseDirectory                     | The HDFS directory that will be used to create temporary files for benchmarking.                        |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| blockSizeMiB                      | Sets the HDFS block size.                                                                               |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| computeNodeHostListFile           | Path to a text file containing host names of all compute nodes (1 per line). This is required if you    |
|                                   | need to start NodeManager to achieve the desired numComputeNodes.                                       |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| examplesJar                       | The Java JAR file for Terasort tests.                                                                   |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| flushCompute                      | If 1, the Linux cache is flushed on all compute nodes prior to the test.                                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| flushIsilon                       | If 1, the Isilon cache is flushed prior to the test. **WARNING: This should not be enabled on           |
|                                   | production systems!**                                                                                   |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| hadoopAuthentication              | "standard" or "kerberos"                                                                                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| hadoopClientHost                  | IP or DNS name of the server with the Hadoop client.                                                    |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| hadoopClientUser                  | User to SSH into the Hadoop client as.                                                                  |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| hadoopParameters                  | Other parameters to pass to the Hadoop command line. This should specify the memory requirements.       |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| hdfsThreads                       | Before starting the test, the Isilon HDFS daemon will be configured to use this many threads.           |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| ioFileBufferSize                  | Corresponds to the Hadoop parameter io.file.buffer.size.                                                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| isilonNodePoolName                | Name of the Isilon node pool used for HDFS. The number of nodes in this pool will be reduced to match   |
|                                   | numIsilonNodes.                                                                                         |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| isiHost                           | Isilon host IP or DNS name. This will be used to submit SSH and web service commands.                   |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| isiUser                           | User to SSH into Isilon as.                                                                             |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| isiPassword                       | Password to authenticate to the Isilon web service.                                                     |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| jobClientJar                      | The Java JAR file for TestDFSIO tests.                                                                  |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| kerberosKeytab                    | Path to .keytab file that allows authentication as kerberosPrincipalName                                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| kerberosPrincipalName             | Kerberos principal name for running tests                                                               |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| mapCores                          | Number of CPU cores to allocate to each map task.                                                       |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| mapMemoryMB                       | Memory to allocate to each map task.                                                                    |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| maxTestAttempts                   | Number of times to attempt this test before giving up and moving to the next test. Default is 1.        |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| numComputeNodes                   | The number of compute nodes to use. YARN NodeManagers will be started or stopped to achieve this count. |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| numIsilonNodes                    | The number of Isilon nodes to use. Excess Isilon nodes will be Smartfailed.                             |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| reduceMemoryMB                    | Memory to allocate to each reduce task.                                                                 |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| resultJsonFilename                | Test results will be append to this file.                                                               |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| tag                               | A user-defined identifier for the test. The generate\*.pl scripts use an incrementing number.           |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| test                              | The type of test to run. Available values are: write, read, teragen, terasort, teravalidate. Write must |
|                                   | precede read. Teragen, terasort, and teravalidate must run in order.                                    |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| yarnServiceControlMethod          | Set to "yarn-daemon.sh" for HDP. Set to "service" for PHD.                                              |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| **DFS IO Parameters (write,       |                                                                                                         |
| read)*                            |                                                                                                         |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| bufferSize                        | The buffer size used by TestDFSIO.                                                                      |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| nrFiles                           | The number of files to use simultaneously. This equals the number of threads. You must have enough map  |
|                                   | slots to allow each map task to run concurrently. The job will ensure that each task runs at exactly    |
|                                   | the right time or it throws an exception if unable to do so.                                            |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| startIOSec                        | I/O will begin this many seconds after the job is submitted. All tasks must have started by this time   |
|                                   | or the job will fail.                                                                                   |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| startMeasurementSec               | Measurement of I/O speed will begin this many seconds after I/O begins.                                 |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| stopAfterSec                      | I/O will stop this many seconds after measurement begins.                                               |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| **Teragen Parameters*             |                                                                                                         |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| dataSizeMB                        | The total size of all files generated.                                                                  |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| mapTasks                          | The number of map tasks that will create the files. This will equal the number of files.                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| **Terasort Parameters*            |                                                                                                         |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| mapOutputCompressCodec            | Set the value of the Hadoop parameter mapred.map.output.compress.codec.                                 |
|                                   | "org.apache.hadoop.io.compress.Lz4Codec" is recommended.                                                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| reduceTasks                       | The number of reduce tasks. In subsequent teravalidate tests, this will be uesd as the number of        |
|                                   | mappers.                                                                                                |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| sortFactor                        | Set the value of the Hadoop parameter io.sort.factor.                                                   |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| sortMiB                           | Set the value of the Hadoop parameter mapreduce.task.io.sort.mb. For best results, make this slightly   |
|                                   | larger than your HDFS block size to avoid spills.                                                       |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
| terasortOutputReplication         | Output files will have this many HDFS block replicas. Default is 1.                                     |
+-----------------------------------+---------------------------------------------------------------------------------------------------------+
