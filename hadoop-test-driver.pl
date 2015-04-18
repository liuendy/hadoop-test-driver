#!/usr/bin/perl
#
my $appname = "hadoop-test-driver";
#
# Written by Claudio Fahey (claudio.fahey@emc.com)
#
# Usage 1:
#   $ ./generate-terasort-tests.pl > terasort.tests
#   $ ./hadoop-test-driver.pl site.cfg terasort.tests
# Usage 2:
#   $ ./generate-dfsio-tests.pl | ./hadoop-test-driver.pl site.cfg -
#

# Enable warnings and strict mode
use strict;
use warnings;

# Load standard modules
use Switch;
use FileHandle;
use Data::Dumper;
use Data::UUID;
use JSON;
use XML::Simple;
use File::Slurp "slurp";
use Error qw(:try); 
use POSIX;
use File::Temp;
use File::Path qw(make_path);

###########################################################################
# Initialize
############################################################################

# Turn on auto flush for STDOUT and STDERR
select(STDERR);
local $| = 1;
select(STDOUT);
local $| = 1;

############################################################################
# Default Parameters
############################################################################

$Data::Dumper::Sortkeys = 1;

our $verbose = 0;
our $noop = 0;
our $logFileName = "$appname.log";
our %defaultConfig = (
    all => {
        flushIsilon => 0,
        flushCompute => 0,
        resultJsonFilename => "$appname.json",
        examplesJar => "/usr/lib/gphd/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        jobClientJar => "/usr/lib/gphd/hadoop-mapreduce/hadoop-mapreduce-client-jobclient.jar",
        customJar => "com.emc.hadoop-tests-0.6.jar",
        shufflePlugIn => "default",
        ioFileBufferSize => 4096,
        hdfsThreads => 64,
        blockSizeMiB => 128,
        mapCores => 1,
        mapMemoryMB => 768,
        maxTestAttempts => 1,
        reduceMemoryMB => 1536,
        appMasterMemoryMB => 1024,
        mapMaxAttempts => 1,
        reduceCores => 1,
        reduceMaxAttempts => 1,
        javaOptsXmxRatio => 0.75,
        sortMiB => 256,
        sortFactor => 100,
        terasortOutputReplication => 1,
        hadoopParameters => "",
        hadoopWsPort => 8088,
        testVariant => 'standard',
        collectPerfDataIsilon => 0,
        perfDataStartMeasurementSec => 60,
        perfDataStopAfterSec => 60,
        hadoopAuthentication => 'standard',
        yarnServiceControlMethod => 'service',
        },
    read => {
        testVariant => 'com.emc.hadoop',
        bufferSize => 1024*1024,
        startIOSec => 45,
        startMeasurementSec => 15,
        # Read tests will last this many seconds
        stopAfterSec => 180,
        nrFiles => 12,
        dataSizeMB => 0,    # no minimum
        mapCores => 0,
        },
    write => {
        testVariant => 'com.emc.hadoop',
        bufferSize => 1024*1024,
        startIOSec => 45,
        startMeasurementSec => 15,
        # Write tests must be longer to ensure that following read test has enough data
        stopAfterSec => 180 * 3,
        nrFiles => 12,
        dataSizeMB => 0,    # no minimum
        mapCores => 0,
        },
    teragen => {
        mapTasks => 24,
        dataSizeMB => 10*1000,
        mapMemoryMB => 2048,
        reduceMemoryMB => 2048,
        },
    terasort => {
        reduceTasks => 24,
        mapMemoryMB => 2048,
        reduceMemoryMB => 2048,
        },
    teravalidate => {
        reduceTasks => 1,
        mapMemoryMB => 2048,
        reduceMemoryMB => 2048,
        },
    grep => {
        reduceTasks => 1,
        mapMemoryMB => 2048,
        reduceMemoryMB => 2048,
        inputDirectory => 'terasort-input',
        outputDirectory => 'grep-output',
        },
    );
our %clusterConfig = ();       # overrides defaultConfig
our %commonConfig = ();        # overrides clusterConfig
our @testList = ();
my @fullTestList = ();

############################################################################
# END of Default Parameters
############################################################################

# Parse command-line parameters
my $haveConfig = 0;
my $mode = "tests";
my $hostlistfile = "";
my $StopSignalFileName = "stop_signal";

for (my $i = 0 ; $i <= $#ARGV ; $i++)
	{
    my $lcarg = $ARGV[$i];
	if ($lcarg eq "--noop")
    	{
		$noop = 1;
        }
    elsif ($lcarg eq "--writehostlist")
        {
        $mode = "writehostlist";
        $i++;
        $hostlistfile = $ARGV[$i];
        }
	elsif ($lcarg eq "--verbose")
    	{
		$verbose = 1;
        }
    elsif ($lcarg eq "-")
        {
        # This is useful when piping from generate*tests.pl.
        print "Loading configuration from STDIN\n";
        my $stdinstr = do { local $/; <STDIN> };
        eval $stdinstr;
		die "Unable to load configuration from STDIN: $@" if $@ || $!;
		push @fullTestList, @testList;  # Append to test list
		$haveConfig = 1;
        }
	elsif ($lcarg =~ /^\-/)
    	{
		die "Unrecognized command-line parameter $lcarg";
        }
    else
		{
		print "Loading configuration file $lcarg\n";
		do $lcarg;
		die "Unable to load configuration file $lcarg" if $@ || $!;
		push @fullTestList, @testList;  # Append to test list
		$haveConfig = 1;
		}
    }
die "No configuration file specified" unless $haveConfig;

# Open log file
my $logFile = FileHandle->new($logFileName, "a");
Print("$appname BEGIN\n");

# Apply defaults, site, and common configuration parameters to test list.
# Apply configuration for "all" test then specific test.
foreach (@fullTestList)
    {
    my $test = $_->{test};
    my $config;
    $config = $defaultConfig{all} // {};
    my %result = %$config;
    $config = $defaultConfig{$test} // {};
    @result{keys %$config} = values %$config;
    $config = $clusterConfig{all} // {};
    @result{keys %$config} = values %$config;
    $config = $clusterConfig{$test} // {};
    @result{keys %$config} = values %$config;
    $config = $commonConfig{all} // {};
    @result{keys %$config} = values %$config;
    $config = $commonConfig{$test} // {};
    @result{keys %$config} = values %$config;
    $config = {%$_};
    @result{keys %$config} = values %$config;
    $_ = \%result;
    }

Print(Data::Dumper->Dump([\@fullTestList], ['*testList'])) if $verbose;
Print("Number of tests: " . scalar(@fullTestList) . "\n");

############################################################################
# Collect configuration details
############################################################################

my $oneFsVersion = "";
my $hadoopVersion = "";
die unless defined($fullTestList[0]{hadoopClientUser});
die unless defined($fullTestList[0]{hadoopClientHost});
if (!$noop)
    {
    if (defined($fullTestList[0]{isiUser}))
        {
        $oneFsVersion = Ssh($fullTestList[0]{isiUser}, $fullTestList[0]{isiHost}, "isi version");
        chomp($oneFsVersion);
        }
    $hadoopVersion = Ssh($fullTestList[0]{hadoopClientUser}, $fullTestList[0]{hadoopClientHost}, "hadoop version | head -1");
    chomp($hadoopVersion);
    }

############################################################################
# Execute tests or alternative mode
############################################################################

my $GlobalErrorCount = 0;
my $GlobalWarningCount = 0;

RunAllTests();

Print("$appname END\n");

############################################################################
# End of main function
############################################################################

sub RunAllTests
    {    
    unlink($StopSignalFileName);
    
    if ($mode eq "writehostlist")
        {
        my $config = $fullTestList[0];
        my @nodeHostNames = GetComputeNodeHostNames($config); 
        my $file = FileHandle->new($hostlistfile, "w");
        foreach my $node (@nodeHostNames)
            {
            print $file "$node\n";
            }    
        }
    elsif ($mode eq "tests")
        {
        my $numTests = scalar(@fullTestList);
        for (my $i = 1 ; $i <= $numTests ; $i++)
	        {
            my $config = $fullTestList[$i-1];
	        $config->{sequenceInTestBatch} = $i;
	        $config->{sizeOfTestBatch} = $numTests;
            $config->{testAttempt} = 0;
            if (!$config->{testUuid})
                {
                my $ug = new Data::UUID;
                $config->{testUuid} = $ug->create_str();
                print "uuid=" . $config->{testUuid} . "\n";
                }
            	        
            my $stop = 0;
	        while ($stop == 0)
	            {
	            try
	                {
                    $config->{testAttempt}++;
                	RunTest($config);
                	$stop = 1;
                	}
	            catch Error with
	                {
	                my $ex = shift;
                    $GlobalWarningCount++;
	                Print("EXCEPTION: " . $ex . "\n");
                    Print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                    if ($config->{testAttempt} >= $config->{maxTestAttempts})
                        {
                        Print("All attempts failed!\n");
                        $GlobalErrorCount++;
                        $stop = 1;
                        }
                    sleep(5);
	                };
	            }
            }
	    Print("*****************************************************************\n");
	    Print("All $numTests tests completed. ($GlobalErrorCount errors, $GlobalWarningCount warnings)\n");
        }
    }
        
sub RunTest
    {
    my ($config) = @_;
    my $test = $config->{test};
    die unless defined($test);

    $config->{utcTimeBegin} = gmtime();

	my $pctComplete = int(100.0 * ($config->{sequenceInTestBatch} - 1) / $config->{sizeOfTestBatch});

	Print("*****************************************************************\n");
	$config->{testDesc} = "Running test '$test': " . $config->{sequenceInTestBatch} . " of " . $config->{sizeOfTestBatch} . " ($pctComplete %)";
	if ($GlobalErrorCount > 0 || $GlobalWarningCount > 0)
	    {
    	$config->{testDesc} .= " (" . $GlobalErrorCount . " errors, " . $GlobalWarningCount . " warnings)";
    	}
	Print($config->{testDesc} . "\n");	

    GetEnvironmentInfo($config);

    Print("Running test '$test' with the following configuration:\n");
    Print(Data::Dumper->Dump([$config], ['config']));

   switch ($test)
        {
        case "localcommand"     {RunLocalCommand($config, $config->{command}, 1);}
        case "hadoopcommand"    {RunHadoopCommand($config, $config->{command}, 1);}
        case "isiloncommand"    {RunIsilonCommand($config, $config->{command}, 1);}
        case "killalljobs"      {KillAllJobs($config);}
        case "read"             {TestDFSIO($config);}
        case "write"            {TestDFSIO($config);}
        case "teragen"          {Teragen($config);}
        case "terasort"         {Terasort($config);}
        case "teravalidate"     {Teravalidate($config);}
        case "grep"             {Grep($config);}
        case "unittest-ConfigureCompute"    {ConfigureCompute($config);}
        case "unittest-ConfigureIsilon"     {ConfigureIsilon($config);}
        else
            {
            throw Error::Simple("Unknown test $test");
            }        
        }
        
    if (-f $StopSignalFileName)
        {
        Print("Stopping due to $StopSignalFileName file.\n");
        exit(1);
        }
    }

sub UndefToEmpty
	{
    my ($x) = @_;
    return $x // "";                                #/
    }

sub Print
    {
    my ($str) = @_;
    return if $str eq "";
    print localtime() . ": " . $str;
    print $logFile localtime() . ": " . $str;
    }

sub System
    {
    my ($cmd) = @_;
    Print "# $cmd\n";
    system($cmd);
    }

sub SystemCommand
    {
    my ($cmd, $print_stdout) = @_;
    $print_stdout = defined($print_stdout) ? $print_stdout : 1;
    Print "# $cmd\n";
    my $stdout = qx($cmd);
    my $exit_code = $?;
    chomp($stdout);
    $stdout .= "\n";
    Print $stdout if $print_stdout;
    Print("Exit code=$exit_code\n");
    return ($exit_code, $stdout);
    }

sub Ssh
    {
    my ($user, $host, $command, $opts) = @_;
    $opts = '' if !defined($opts);
    my $cmd = "ssh $opts $user\@$host \"$command\" 2>&1";
    Print "# $cmd\n";
    my $stdout = qx($cmd);
    my $exit_code = $?;
    chomp($stdout);
    $stdout .= "\n";
    Print $stdout;
    Print("Exit code=$exit_code\n");
    return ($exit_code, $stdout);
    }

sub GetRegEx
    {
    my ($haystack, $needle) = @_;
    if ($haystack =~ m/$needle/)
        {
        return $1;
        }
    return undef;
    }

sub CallWebService
    {
    my ($url, $username, $password) = @_;
    my $cmd = "curl --insecure --silent";
    $cmd .= " --user $username:$password" if defined($password);
    $cmd .= " $url";
    my ($exit_code, $stdout) = SystemCommand($cmd, 0);
    throw Error::Simple("Error calling web service $url") if $exit_code != 0;    
    my $result = decode_json($stdout);
    Print(Data::Dumper->Dump([$result], ['CallWebService_result'])) if $verbose;
    return $result;    
    }
    
sub TryCallWebService
    {
    my ($url, $username, $password) = @_;
    try
        {
        return CallWebService($url, $username, $password);
        }
    catch Error with
        {
        return undef;
        }       
    }

sub CallWebServiceXML
    {
    my ($url, $username, $password) = @_;
    my $cmd = "curl --insecure --silent";
    $cmd .= " --user $username:$password" if defined($password);
    $cmd .= " $url";
    my ($exit_code, $stdout) = SystemCommand($cmd, 0);
    throw Error::Simple("Error calling web service $url") if $exit_code != 0;    
    my $result = XMLin($stdout, KeyAttr => []);
    Print(Data::Dumper->Dump([$result], ['CallWebServiceXML_result'])) if $verbose;
    return $result;    
    }

sub GetComputeNodeInfo
    {
    my ($config) = @_;
    my $baseYarnUrl = "http://" . $config->{hadoopClientHost} . ":" . $config->{hadoopWsPort};
    my $info = CallWebService("$baseYarnUrl/ws/v1/cluster/nodes");
    $config->{computeNodeInfo} = $info;
    $config->{hadoopResourceManagerConf} = CallWebServiceXML("$baseYarnUrl/conf");
    return $info;
    }

# Get active compute nodes.
sub GetComputeNodeHostNames
    {
    my ($config) = @_;
    my $info = GetComputeNodeInfo($config);
    my @hostNames = ();
    foreach my $node (@{$info->{nodes}->{node}})
        {
        if ($node->{state} eq 'RUNNING')
            {
            push(@hostNames, $node->{nodeHostName});
            }
        }
    return sort(@hostNames);
    }

# Get all compute nodes, including inactive ones.
sub GetAllComputeNodeHostNames
    {
    my ($config) = @_;
    throw Error::Simple("computeNodeHostListFile not defined") if !defined($config->{computeNodeHostListFile});
    my $contents = slurp($config->{computeNodeHostListFile});
    return split(/\s+/, $contents);
    }
    
sub GetNumComputeNodes
    {
    my ($config) = @_;
    my @nodes = GetComputeNodeHostNames($config);
    my $count = scalar(@nodes);
    Print("GetNumComputeNodes=$count\n");
    return scalar($count);
    }

sub GetCompletedJobInfo
    {
    my ($config) = @_;
    if (defined($config->{hadoopJobId}))
        {
        my $baseHistoryUrl = "http://" . $config->{hadoopClientHost} . ":19888";
        my $jobHistoryUrl = "$baseHistoryUrl/ws/v1/history/mapreduce/jobs/" . $config->{hadoopJobId};
        $config->{hadoopJobInfo} = TryCallWebService($jobHistoryUrl);
        $config->{hadoopJobConf} = TryCallWebService("$jobHistoryUrl/conf");
        $config->{hadoopJobCounters} = TryCallWebService("$jobHistoryUrl/counters");
        $config->{hadoopJobTasks} = TryCallWebService("$jobHistoryUrl/tasks");
        }
    CollectMapRedLogs($config);
    }

# This script must run directly on a Hadoop node since it runs "mapred".
sub CollectMapRedLogs
    {
    my ($config) = @_;
    if (defined($config->{mapRedLogDir}) && defined($config->{hadoopJobId}))
        {
        my $logDir = File::Temp->newdir();
        my $jobId = $config->{hadoopJobId};
        make_path($config->{mapRedLogDir});
        my $mapRedLogArchiveFile = $config->{mapRedLogDir} . "/$jobId--" . $config->{testUuid} . ".tar.bz2";
        System("mapred job -list-attempt-ids $jobId MAP    completed | xargs -P 50 -n 1 -I{} sh -c \"mapred job -logs $jobId {} > $logDir/{}.log\"");
        System("mapred job -list-attempt-ids $jobId REDUCE completed | xargs -P 50 -n 1 -I{} sh -c \"mapred job -logs $jobId {} > $logDir/{}.log\"");
        System("mapred job -logs $jobId > $logDir/appmaster.log");
        System("tar -cjvf $mapRedLogArchiveFile -C $logDir .");
        $config->{mapRedLogArchiveFile} = $mapRedLogArchiveFile;
        }
    }
    
sub GetIsilonNodeInfo
    {
    my ($config) = @_;
    my $baseIsilonUrl = "https://" . $config->{isiHost} . ":8080";
    my $info = CallWebService("$baseIsilonUrl/platform/1/storagepool/nodepools/" . ($config->{isilonNodePoolName} // ""), $config->{isiUser}, $config->{isiPassword});        #/
    $config->{isilonNodeInfo} = $info;
    return $info;
    }

sub GetIsilonNodeIds
    {
    my ($config) = @_;
    my $info = GetIsilonNodeInfo($config);
    return sort(@{$info->{nodepools}->[0]->{lnns}});
    }
    
sub GetNumIsilonNodes
    {
    my ($config) = @_;
    my @nodes = GetIsilonNodeIds($config);
    my $count = scalar(@nodes);
    Print("GetNumIsilonNodes=$count\n");
    return scalar($count);
    }

sub GetEnvironmentInfo
    {
    my ($config) = @_;
    $config->{oneFsVersion} = $oneFsVersion;
    $config->{hadoopVersion} = $hadoopVersion;
    }

# Returns true if current Isilon version is equal or greater than specified version.
sub HaveMinimumIsilonVersion
    {
    my ($config, $minVerMajor, $minVerMinor) = @_;
    my $curVerMajor = 0 + GetRegEx($config->{oneFsVersion}, "v(\\d+)\\.");
    my $curVerMinor = 0 + GetRegEx($config->{oneFsVersion}, "v\\d+\\.(\\d+)\\.");
    #Print("curVerMajor=$curVerMajor\n");
    #Print("curVerMinor=$curVerMinor\n");
    return ($minVerMajor < $curVerMajor) || (($minVerMajor == $curVerMajor) && ($minVerMinor <= $minVerMinor));
    }

sub ConfigureCompute
    {
    my ($config) = @_;
    return if $noop;

    my $NMStartCmd;
    my $NMStopCmd;
    my $RMRestartCmd;
    my $SshOpts = "";
    if ($config->{yarnServiceControlMethod} eq "yarn-daemon.sh")
        {
        # Below commands are used by Ambari on HDP 2.1.
        $NMStartCmd   = "sudo -u yarn HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec /usr/lib/hadoop-yarn/sbin/yarn-daemon.sh --config /etc/hadoop/conf start nodemanager";
        $NMStopCmd    = "sudo -u yarn HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec /usr/lib/hadoop-yarn/sbin/yarn-daemon.sh --config /etc/hadoop/conf stop  nodemanager";
        $RMRestartCmd = "sudo -u yarn HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec /usr/lib/hadoop-yarn/sbin/yarn-daemon.sh --config /etc/hadoop/conf stop  resourcemanager"
                   . " ; sudo -u yarn HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec /usr/lib/hadoop-yarn/sbin/yarn-daemon.sh --config /etc/hadoop/conf start resourcemanager";
        $SshOpts      = "-tt";  # This is required to be able to do ssh sudo. This will result in warning message "tcgetattr: Invalid argument" which can be ignored.
        }
    else
        {
        # Below commands are used by PHD 2.1.
        $NMStartCmd   = "service hadoop-yarn-nodemanager     start";
        $NMStopCmd    = "service hadoop-yarn-nodemanager     stop";
        $RMRestartCmd = "service hadoop-yarn-resourcemanager stop ; service hadoop-yarn-resourcemanager start";        
        }

    if (defined($config->{numComputeNodes}))
        {
        # numComputeNodes specified; change environment as necessary
        for (;;)
            {
            my @nodeHostNames = GetComputeNodeHostNames($config); 
            my $currentComputeNodes = scalar(@nodeHostNames);
            my $excessNodes = $currentComputeNodes - $config->{numComputeNodes};
            if ($excessNodes == 0)
                {
                last;
                }
            elsif ($excessNodes > 0)
                {
                Print("Reducing number of compute nodes by $excessNodes.\n");                
                # Stop the node manager on the hosts in reverse order.
                foreach my $node (reverse(@nodeHostNames))
                    {
                    Print("Stopping node $node\n");
                    my ($exit_code, $output) = Ssh("root", $node, $NMStopCmd, $SshOpts);
                    $excessNodes--;
                    last if $excessNodes == 0;
                    }
                # Restart Resource Manager to reflect node count immediately
                my ($exit_code, $output) = Ssh("root", $config->{hadoopClientHost}, $RMRestartCmd, $SshOpts);
                }
            else
                {
                # We need to increase compute nodes. We simply start all known compute nodes and then shutdown the excess nodes
                # in the next loop iteration.
                Print("Starting all compute nodes.\n");
                foreach my $node (GetAllComputeNodeHostNames($config))
                    {
                    Print("Starting node $node\n");
                    my ($exit_code, $output) = Ssh("root", $node, $NMStartCmd, $SshOpts);
                    }
                }
            # Wait for the resource manager to get the correct node count.
            Print("Waiting for Resource Manager to initialize.\n");
            sleep(45);
            }
        }
    else
        {
        # numComputeNodes not specified; determine current value from environment
        $config->{numComputeNodes} = GetNumComputeNodes($config);
        }
    $config->{numPhysicalComputeNodes} = $config->{numComputeNodes} / $config->{numComputeNodesPerPhysicalComputeNode} if defined($config->{numComputeNodesPerPhysicalComputeNode});    

    Print("ConfigureCompute: numComputeNodes=" . $config->{numComputeNodes} . "\n");
    Print("ConfigureCompute: numPhysicalComputeNodes=" . $config->{numPhysicalComputeNodes} . "\n");
    Print("ConfigureCompute: numComputeNodesPerPhysicalComputeNode=" . $config->{numComputeNodesPerPhysicalComputeNode} . "\n");
    
    FlushComputeCache($config);
    }

sub ConfigureIsilon
    {
    my ($config) = @_;

    return if $noop;
    return if !defined($config->{isiHost});
    
    if (defined($config->{numIsilonNodes}))
        {
        # numIsilonNodes specified; change environment as necessary
        for (;;)
            {
            my @nodeIds = GetIsilonNodeIds($config);
            my $currentNodes = scalar(@nodeIds);
            my $excessNodes = $currentNodes - $config->{numIsilonNodes};
            if ($excessNodes == 0)
                {
                last;
                }
            elsif ($excessNodes > 0)
                {
                my $newNodeCount = $currentNodes - $excessNodes;
                my $minNodesForQuorum = ceil(($currentNodes + 1.0)/2.0);
                $newNodeCount = $minNodesForQuorum if $newNodeCount < $minNodesForQuorum;
                my $failNodes = $currentNodes - $newNodeCount;
                $failNodes <= $excessNodes or die;
                    
                Print("Reducing number of Isilon nodes by $excessNodes.\n");

                # Delete all benchmark data to make smartfail faster
                DeleteDirectory($config, "hdfs://" . $config->{isiHost} . ":8020/benchmarks/*/*");
                
                # Smartfail nodes beginning with the highest node ID (node #).
                my @nodeIdsToFail = reverse(@nodeIds);
                @nodeIdsToFail = @nodeIdsToFail[0 .. $failNodes-1];
                scalar(@nodeIdsToFail) == $failNodes or die;
                foreach my $node (@nodeIdsToFail)
                    {
                    Print("Smartfailing node $node\n");
                    my $cmd = "echo yes | isi devices --action smartfail --device $node";
                    my ($exit_code, $output) = RunIsilonCommand($config, $cmd);
                    }
                }
            else
                {
                throw Error::Simple("Isilon nodes must be added manually");
                }
            # Nodes are still being smartfailed. We will check periodically until the count is correct.
            sleep(30);
            }
        }
    else
        {
        # numIsilonNodes not specified; determine current value from environment
        $config->{numIsilonNodes} = GetNumIsilonNodes($config);
        }

    Print("ConfigureIsilon: numIsilonNodes=" . $config->{numIsilonNodes} . "\n");

    if ($config->{blockSizeMiB})
        {
        my $cmd;
        if (HaveMinimumIsilonVersion($config, 7, 1))
            {
            $cmd = "isi hdfs settings modify --default-block-size " . $config->{blockSizeMiB} . "MB";
            }
        else
            {
            $cmd = "isi hdfs --block-size=" . $config->{blockSizeMiB} . "MB";
            }
        if (!RunIsilonCommand($config, $cmd))
            {
            throw Error::Simple("Unable to set Isilon HDFS block size");
            }
        }

    if ($config->{hdfsThreads})
        {
        my $cmd;
        if (HaveMinimumIsilonVersion($config, 7, 1))
            {
            $cmd = "isi hdfs settings modify --server-threads " . $config->{hdfsThreads};
            }
        else
            {
            $cmd = "isi hdfs --num-threads=" . $config->{hdfsThreads};
            }
        if (!RunIsilonCommand($config, $cmd))
            {
            throw Error::Simple("Unable to set Isilon HDFS threads");
            }
        }

    if ($config->{collectPerfDataIsilon})
        {
        if (!RunIsilonCommand($config, "mkdir -p /ifs/pcdata ; isi services -a isi_ph_rpcd enable"))
            {
            throw Error::Simple("Unable to enable Isilon performance collector daemon");
            }
        }
        
    FlushIsilonCache($config);
    }
    
sub GetHadoopParameters
    {
    my ($config) = @_;
    my $options = 
        " " . $config->{hadoopParameters} .
        " -Ddfs.blocksize=" . $config->{blockSizeMiB} . "M" .
        " -Dio.file.buffer.size=" . $config->{ioFileBufferSize} .
        " -Dmapreduce.map.cpu.vcores=" .$config->{mapCores} .
        " -Dmapreduce.map.java.opts=-Xmx" . int($config->{mapMemoryMB} * $config->{javaOptsXmxRatio}) . "m" .
        " -Dmapreduce.map.maxattempts=" . $config->{mapMaxAttempts} .
        " -Dmapreduce.map.memory.mb=" . $config->{mapMemoryMB} .
        " -Dmapreduce.map.output.compress=" . ($config->{mapOutputCompressCodec} ? "true" : "false") .
        " -Dmapreduce.map.output.compress.codec=" . ($config->{mapOutputCompressCodec} // "") .                            #/
        " -Dmapreduce.reduce.cpu.vcores=" .$config->{reduceCores} .
        " -Dmapreduce.reduce.java.opts=-Xmx" . int($config->{reduceMemoryMB} * $config->{javaOptsXmxRatio}) . "m" .
        " -Dmapreduce.reduce.maxattempts=" . $config->{reduceMaxAttempts} .
        " -Dmapreduce.reduce.memory.mb=" . $config->{reduceMemoryMB} .
        " -Dmapreduce.task.io.sort.factor=" . $config->{sortFactor} .
        " -Dmapreduce.task.io.sort.mb=" . $config->{sortMiB} .
        " -Dyarn.app.mapreduce.am.command-opts=-Xmx" . int($config->{appMasterMemoryMB} * $config->{javaOptsXmxRatio}) . "m" .
        " -Dyarn.app.mapreduce.am.resource.mb=" . $config->{appMasterMemoryMB} .
        "";

    $options .= " -Dmapred.map.tasks=" . $config->{mapTasks} if defined($config->{mapTasks});
    $options .= " -Dmapred.reduce.tasks=" . $config->{reduceTasks} if defined($config->{reduceTasks});

    if ($config->{shufflePlugIn} eq "Claudio")
        {
        $options .=
            " -Dclaudio.shuffle.directory=" . $config->{shuffleDirectory} .
            " -Dmapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.ClaudioMapOutputCollector" .
            " -Dmapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.ClaudioShuffle" .
            " -Dmapreduce.job.reduce.slowstart.completedmaps=0.95";
        }
    return $options;
    }

sub RunLocalCommand
    {
    my ($config, $cmd, $record_result) = @_;
    die unless defined($cmd);
    if ($noop)
        {
        print "# $cmd\n";
        }
    else
        {
        my ($exit_code, $output) = SystemCommand($cmd);
        RecordResult($config, {output => $output, exitCode => $exit_code}) if $record_result;
        return ($exit_code == 0);
        }
    }

sub RunHadoopCommand
    {
    my ($config, $cmd, $record_result) = @_;
    die unless defined($cmd);
    if ($noop)
        {
        print "# ssh $cmd\n";
        return 1;
        }    
    else
        {
        my ($exit_code, $output) = Ssh($config->{hadoopClientUser}, $config->{hadoopClientHost}, $cmd);
        RecordResult($config, {output => $output, exitCode => $exit_code}) if $record_result;
        return ($exit_code == 0);
        }
    }

sub RunIsilonCommand
    {
    my ($config, $cmd, $record_result) = @_;
    die unless defined($config->{isiUser});
    die unless defined($config->{isiHost});
    die unless defined($cmd);
    if ($noop)
        {
        print "# ssh $cmd\n";
        }    
    else
        {
        my ($exit_code, $output) = Ssh($config->{isiUser}, $config->{isiHost}, $cmd);
        RecordResult($config, {output => $output, exitCode => $exit_code}) if $record_result;
        return ($exit_code == 0);
        }
    return 1;
    }

sub HadoopAuthenticate
    {
    my ($config) = @_;
    if ($config->{hadoopAuthentication} eq 'kerberos')
        {
        die unless defined($config->{kerberosKeytab});
        die unless defined($config->{kerberosPrincipalName});
        my $cmd = "kinit -kt " . $config->{kerberosKeytab} . ' ' . $config->{kerberosPrincipalName};
        if (!RunHadoopCommand($config, $cmd))
            {
            throw Error::Simple("Unable to initialize Kerberos");
            }
        }
    }

sub KillAllJobs
    {
    my ($config) = @_;
    HadoopAuthenticate($config);
    my $cmd = "mapred job -list | grep job_ | awk ' { system(\\\"mapred job -kill \\\" \\\$1) } '";
    if (!RunHadoopCommand($config, $cmd, 1))
        {
        throw Error::Simple("Unable to kill all jobs");
        }
    }

sub DeleteDirectory
    {
    my ($config, $dir) = @_;
    HadoopAuthenticate($config);
    my $cmd = "hadoop fs -rm -r -f -skipTrash $dir";
    if (!RunHadoopCommand($config, $cmd))
        {
        throw Error::Simple("Unable to delete directory $dir");
        }
    }

sub FlushIsilonCache
    {
    my ($config) = @_;
    if ($config->{flushIsilon})
        {
        if (!RunIsilonCommand($config, "isi_for_array isi_flush"))
            {
            throw Error::Simple("Unable to flush Isilon");
            }
        }
    }

sub FlushComputeCache
    {
    my ($config) = @_;
    if ($config->{flushCompute})
        {
        my @nodeHostNames = GetComputeNodeHostNames($config);
        foreach my $node (@nodeHostNames)
            {
            Print("Flushing caches on $node\n");
            my $cmd = "sync ; sysctl -w vm.drop_caches=3";
            my ($exit_code, $output) = Ssh("root", $node, $cmd);
            if ($exit_code)
                {
                throw Error::Simple("Unable to drop caches on $node");
                }
            }        
        }
    }

sub StartPerfDataCollector
    {
    # This will start the performance collector and then return.
    my ($config) = @_;
    if ($config->{collectPerfDataIsilon})
        {
        $config->{perfDataDir} = "/ifs/pcdata/" . $config->{test} . "-" . $config->{testUuid};
        my $cmd = "(sleep " . $config->{perfDataStartMeasurementSec} . " ; isi_ph_pc -d " . $config->{perfDataDir} . " -t " .
                  $config->{perfDataStopAfterSec} . " \\`isi_nodes %{internal}\\`) > /dev/null 2>&1 &";
        if (!RunIsilonCommand($config, $cmd))
            {
            throw Error::Simple("Unable to start Isilon performance collection");
            }
        }
    }

sub SkipTest
    {
    my ($config) = @_;
    if (defined($config->{skipIfHadoopFileFound}))
        {
        if (RunHadoopCommand($config, "hadoop fs -test -e " . $config->{skipIfHadoopFileFound}))
            {
            # File found. Skip this test.
            Print("Skipping this test because the following file exists: " . $config->{skipIfHadoopFileFound} . "\n");
            return 1;
            }
        }
    return 0;
    }

sub RunHadoopJob
    {
    my ($config) = @_;
    die unless defined($config->{hadoopCommand});

	Print("-----------------------------------------------------------------\n");
	Print($config->{testDesc} . "\n");
	Print("-----------------------------------------------------------------\n");

    if ($noop)
        {
        print "# ssh $config->{hadoopCommand}\n";
        return (1, "");
        }    
    else
        {
        HadoopAuthenticate($config);
        StartPerfDataCollector($config);
        my $time0 = time;
        my ($exit_code, $output) = Ssh($config->{hadoopClientUser}, $config->{hadoopClientHost}, $config->{hadoopCommand});
        my $elapsed_sec = time - $time0;
        Print "elapsed_sec=$elapsed_sec\n";            
        $config->{hadoopCommandOutput} = $output;
        $config->{hadoopCommandExitCode} = $exit_code;
        $config->{error} = ($exit_code != 0) ? 1 : 0;
        $config->{elapsedSec} = $elapsed_sec;
        $config->{bytesReadHDFS} = GetRegEx($output, "Bytes Read=(.*)");
        $config->{bytesWrittenHDFS} = GetRegEx($output, "Bytes Written=(.*)");
        $config->{hadoopJobId} = GetRegEx($output, "Running job: (job_[0-9_]+)");
        GetCompletedJobInfo($config);
        return ($exit_code, $output);
        }    
    }
sub TestDFSIO
    {
    my ($config) = @_;
    die unless defined($config->{hadoopClientUser});
    die unless defined($config->{hadoopClientHost});
    die unless defined($config->{baseDirectory});

    ConfigureIsilon($config);           # This may delete TestDFSIO
    ConfigureCompute($config);
    return if SkipTest($config);

    my $genericOptions = 
        " " . GetHadoopParameters($config) .
        " -Dio.file.buffer.size=" . $config->{ioFileBufferSize} .
        " -Dtest.build.data=" . $config->{baseDirectory} . "/TestDFSIO" .
        "";
    my $options = 
        " -nrFiles " . $config->{nrFiles} .
        " -bufferSize " . $config->{bufferSize} .
        "";

    if ($config->{testVariant} eq "com.emc.hadoop")
        {
        # Custom TestDFSIO based on exact measurement time and minimum file size.
        die unless defined($config->{startIOSec});
        die unless defined($config->{startMeasurementSec});
        die unless defined($config->{stopAfterSec});
        $config->{jobName} = "TestDFSIO_" . $config->{test} . "," . $config->{nrFiles} . "," . $config->{stopAfterSec};
        $config->{fileSizeMB} = int($config->{dataSizeMB} / $config->{nrFiles});
        $options .=
            " -size " . $config->{fileSizeMB} . "MB";
        $genericOptions .=
            " -DstartIOSec=" . $config->{startIOSec} .
            " -DstartMeasurementSec=" . $config->{startMeasurementSec} .
            " -DstopAfterSec=" . $config->{stopAfterSec};
        $config->{jar} = ($config->{jar}) // ($config->{customJar});                #/
        $config->{perfDataStartMeasurementSec} = $config->{startIOSec} + $config->{startMeasurementSec};
        }
    else
        {
        # Original TestDFSIO based on file size.
        $config->{startIOSec} = undef;
        $config->{startMeasurementSec} = undef;
        $config->{stopAfterSec} = undef;
        $config->{jobName} = "TestDFSIO_" . $config->{test} . "," . $config->{nrFiles} . "," . $config->{dataSizeMB} . "MB";  
        $config->{fileSizeMB} = int($config->{dataSizeMB} / $config->{nrFiles});
        $options .=
            " -size " . $config->{fileSizeMB} . "MB";
        $config->{jar} = ($config->{jar}) // ($config->{jobClientJar});             #/
        }

    $genericOptions .= " -Dmapreduce.job.name=" . $config->{jobName};
    $options .= " -" . $config->{test};
    
    my $cmd = 
        "hadoop jar " . $config->{jar} . " TestDFSIO" . 
        $genericOptions .
        $options .
        "";
        
    $config->{hadoopCommand} = $cmd;

    my ($exit_code, $output) = RunHadoopJob($config);
    if ($exit_code == 0)
        {
        $config->{actualDataSizeMB} = GetRegEx($output, "Total MBytes processed: (.*)");
        if ($config->{testVariant} eq "com.emc.hadoop")
            {
            $config->{totalIoRateMBPerSec} = GetRegEx($output, "Total IO rate MB/sec: (.*)");
            $config->{stdDevPercent} = GetRegEx($output, "IO rate std dev %: (.*)");
            }
        else
            {          
            $config->{totalIoRateMBPerSec} = $config->{actualDataSizeMB} / $config->{elapsedSec};
            }
        }
    RecordResult($config);
    throw Error::Simple("Hadoop job failed") if $exit_code;
    }

sub Teragen
    {
    my ($config) = @_;
    die unless defined($config->{hadoopClientUser});
    die unless defined($config->{hadoopClientHost});
    die unless defined($config->{baseDirectory});

    ConfigureIsilon($config);           # This may delete terasort-input
    ConfigureCompute($config);
    return if SkipTest($config);
    
    DeleteDirectory($config, $config->{baseDirectory} . "/terasort-input");
    
    $config->{jar} = ($config->{jar}) // ($config->{examplesJar});      #/

    my $genericOptions = GetHadoopParameters($config);
    my $options = "";

    my $recsize = 100;
    my $recs = int(($config->{dataSizeMB}) * 1000.0 * 1000.0 / $recsize);
    
    my $cmd = 
        "hadoop jar " . $config->{jar} . " teragen" . 
        $genericOptions .
        $options .
        " $recs " .
        $config->{baseDirectory} . "/terasort-input" .
        "";

    $config->{hadoopCommand} = $cmd;

    my ($exit_code, $output) = RunHadoopJob($config);
    if ($exit_code == 0)
        {
        $config->{dataSizeMB} = $config->{bytesWrittenHDFS} / 1000.0 / 1000.0;
        $config->{totalIoRateMBPerSec} = $config->{dataSizeMB} / $config->{elapsedSec};     # MB/sec
        }
    RecordResult($config);
    throw Error::Simple("Hadoop job failed") if $exit_code;
    }

sub Terasort
    {
    my ($config) = @_;
    die unless defined($config->{hadoopClientUser});
    die unless defined($config->{hadoopClientHost});
    die unless defined($config->{baseDirectory});

    ConfigureIsilon($config);           # This may delete terasort-input
    ConfigureCompute($config);
    return if SkipTest($config);

    DeleteDirectory($config, $config->{baseDirectory} . "/terasort-output");

    $config->{jar} = ($config->{jar}) // ($config->{examplesJar});      #/

    my $genericOptions = GetHadoopParameters($config);
    my $options = "";
    
    $genericOptions .= " -Dmapreduce.terasort.output.replication=" . $config->{terasortOutputReplication};
    
    my $cmd = 
        "hadoop jar " . $config->{jar} . " terasort" .
        $genericOptions .
        $options .
        " " .
        $config->{baseDirectory} . "/terasort-input " .
        $config->{baseDirectory} . "/terasort-output " .
        "";

    $config->{hadoopCommand} = $cmd;
    
    my ($exit_code, $output) = RunHadoopJob($config);
    if ($exit_code == 0)
        {
        $config->{dataSizeMB} = $config->{bytesReadHDFS} / 1000.0 / 1000.0;
        $config->{totalIoRateMBPerSec} = $config->{dataSizeMB} / $config->{elapsedSec};     # MB/sec
        }
    RecordResult($config);
    throw Error::Simple("Hadoop job failed") if $exit_code;
    }

sub Teravalidate
    {
    my ($config) = @_;
    die unless defined($config->{hadoopClientUser});
    die unless defined($config->{hadoopClientHost});
    die unless defined($config->{baseDirectory});

    ConfigureIsilon($config);           # This may delete terasort-output
    ConfigureCompute($config);
    return if SkipTest($config);

    DeleteDirectory($config, $config->{baseDirectory} . "/terasort-report");

    $config->{jar} = ($config->{jar}) // ($config->{examplesJar});      #/

    my $genericOptions = GetHadoopParameters($config);
    my $options = "";
    
    my $cmd = 
        "hadoop jar " . $config->{jar} . " teravalidate" .
        $genericOptions .
        $options .
        " " .
        $config->{baseDirectory} . "/terasort-output " .
        $config->{baseDirectory} . "/terasort-report " .
        "";

    $config->{hadoopCommand} = $cmd;
    
    my ($exit_code, $output) = RunHadoopJob($config);
    if ($exit_code == 0)
        {
        $config->{dataSizeMB} = $config->{bytesReadHDFS} / 1000.0 / 1000.0;
        $config->{totalIoRateMBPerSec} = $config->{dataSizeMB} / $config->{elapsedSec};     # MB/sec
        }
    RecordResult($config);
    throw Error::Simple("Hadoop job failed") if $exit_code;
    }

sub PrepareForHadoopTest
    {
    my ($config) = @_;
    die unless defined($config->{hadoopClientUser});
    die unless defined($config->{hadoopClientHost});
    die unless defined($config->{baseDirectory});

    ConfigureIsilon($config);           # This may delete terasort-output
    ConfigureCompute($config);
    }

sub Grep
    {
    my ($config) = @_;    
    PrepareForHadoopTest($config);
    return if SkipTest($config);

    DeleteDirectory($config, $config->{baseDirectory} . "/" . $config->{outputDirectory});

    $config->{jar} = ($config->{jar}) // ($config->{examplesJar});      #/

    my $genericOptions = GetHadoopParameters($config);
    my $options = "";
    
    my $cmd = 
        "hadoop jar " . $config->{jar} . " grep" .
        $genericOptions .
        $options .
        " " .
        $config->{baseDirectory} . "/" . $config->{inputDirectory} . " " .
        $config->{baseDirectory} . "/" . $config->{outputDirectory} . " " .
        "87iwW5X1K60p7q0h " .        # regex that should not match anything
        "";

    $config->{hadoopCommand} = $cmd;
    
    my ($exit_code, $output) = RunHadoopJob($config);
    if ($exit_code == 0)
        {
        $config->{dataSizeMB} = $config->{bytesReadHDFS} / 1000.0 / 1000.0;
        $config->{totalIoRateMBPerSec} = $config->{dataSizeMB} / $config->{elapsedSec};     # MB/sec
        }
    RecordResult($config);
    throw Error::Simple("Hadoop job failed") if $exit_code;
    }

sub RecordResult
    {
    my ($config, $result) = @_;

    # merge $config and $result into $record
    my %record = %$config;
    @record{keys %$result} = values %$result if (defined($result));
    $record{localTime} = localtime();
    $record{utcTime} = gmtime();

    if (defined($config->{resultJsonFilename}))
        {
        Print("Recording results to file " . $config->{resultJsonFilename} . "\n");
        AppendJsonRecord(\%record, glob($config->{resultJsonFilename})) if !$noop;
        }        
    }

# Append records to a JSON file.
sub AppendJsonRecords
    {
    my ($records, $fileName) = @_;
    my $recs = [];
    
    # Read existing file
    if (-f $fileName)
        {
        my $oldFileText = slurp($fileName);
        $recs = decode_json($oldFileText);
        }

    # Append to end
    push @$recs, @$records;
    
    my $newFileText = JSON->new->utf8->pretty->encode($recs) . "\n";

    # Write temp file and rename
    my $tmpFileName = "$fileName.tmp";
    my $tmpFile = FileHandle->new($tmpFileName, "w") || die "Unable to open JSON file $tmpFileName";
    #Print("tmpFileName=$tmpFileName\n");
    #Print("tmpFile=$tmpFile\n");
    #Print("newFileText=$newFileText\n");
    print $tmpFile $newFileText;
    $tmpFile->close();
    rename $tmpFileName, $fileName;
    }

# Append single record to a JSON file.
sub AppendJsonRecord
    {
    my ($record, $fileName) = @_;
    AppendJsonRecords([$record], $fileName)
    }

