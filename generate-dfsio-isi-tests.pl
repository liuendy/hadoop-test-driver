#!/usr/bin/perl
#
# Usage:
#   $ ./generate-dfsio-tests.pl | ./hadoop-test-driver.pl phd21.cfg -
#
# Required Hadoop settings:
#   yarn-site.xml:
#        "yarn.nodemanager.pmem-check-enabled" : false
#        "yarn.nodemanager.vmem-check-enabled" : false

use strict;
use warnings;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;

my @testList = ();

# Kill any jobs before tests.
push(@testList, {
    test => "killalljobs",
});

foreach my $numIsilonNodes (4) {                    # Must only decrease by amount that maintains quorum. Increase must be done manually.
foreach my $repeat (1..3) {
foreach my $numComputeNodes (11, 8, 6) {   # should normally decrease # 11, 8, 6
foreach my $blockSizeMiB (512) {                    # no effect
foreach my $hdfsThreads (256) {                     # should always be 256
foreach my $ioFileBufferSizeKiB (128) {             # no noticable effect
foreach my $protectionLevel ("21") {                # "21", "1", "2x", "3x"
foreach my $dataAccessPattern ("streaming") {
foreach my $smartCache (1) {                        # best is 1 (enabled)
foreach my $mapTasksPerIsilonNode (18, 32, 10, 56, 4, 100) { #4, 32, 56, 170, 100, 10, 18
foreach my $testVariant ('com.emc.hadoop') {        # 'com.emc.hadoop','standard'

    my $nrFiles = int($mapTasksPerIsilonNode * $numIsilonNodes);

    # Estimate time it takes to start all tasks ($startIOSec) as line through the following two points.
    my $nrFiles1 =   0.0;       my $startIOSec1 =   30.0;
    my $nrFiles2 = 512.0;       my $startIOSec2 = 3*60.0;
    my $startIOSec = int($startIOSec1 + ($startIOSec2 - $startIOSec1)/($nrFiles2 - $nrFiles1) * $nrFiles);
    
    # Ramp up duration - I/O occurs but we don't measure yet.
    my $startMeasurementSec = 15;
    
    # Duration of measurement in seconds.
    my $stopAfterSecRead = 3*60;
    my $stopAfterSecWrite = 3*60;

    # Minimum MB to write to ensure that subsequent read has enough data.
    # The value on the next line must be large enough so that the fastest task on a node is not faster than this rate divided by the number of tasks on the node.
    # Increase this value if read tests fail with exception "Got to end of file before stop time".
    my $maxReadMBPerSecPerIsilonNode = 2500.0;
    my $actualReadSec = $startMeasurementSec + $stopAfterSecRead;  
    my $minWriteMB = $maxReadMBPerSecPerIsilonNode * $actualReadSec * $numIsilonNodes;
    
    my $baseDirectory = "/benchmarks/";
    $baseDirectory .= "$dataAccessPattern-$protectionLevel";
    $baseDirectory .= "-0" if !$smartCache;
    $baseDirectory .= "/hduser1";

    my $common = {
        appMasterMemoryMB => 4096,
        baseDirectory => $baseDirectory,
        blockSizeMiB => $blockSizeMiB,
        collectPerfDataIsilon => 0,
        dataAccessPattern => $dataAccessPattern,
        hdfsThreads => $hdfsThreads,
        ioFileBufferSize => $ioFileBufferSizeKiB*1024,
        mapMemoryMB => 28,      # Note that task will use a lot more virtual memory than this amount.
        maxTestAttempts => 3,
        nrFiles => $nrFiles,
        numComputeNodes => $numComputeNodes,
        numIsilonNodes => $numIsilonNodes,
        protectionLevel => $protectionLevel,
        smartCache => $smartCache,
        sortMiB => 1,
        testVariant => $testVariant,
        };
        
    my $t;

    if ($testVariant eq "com.emc.hadoop") {
        # com.emc.hadoop TestDFSIO
        # Write (measurement only)
        foreach my $repeat_write (1..0) {
            $t = {%$common};
            $t->{test} = "write";
            $t->{startIOSec} = $startIOSec;
            $t->{stopAfterSec} = $stopAfterSecWrite;    # measurement stops after this many seconds
            push(@testList, $t);
        }
        
        # Write (measurement + prepare for read)
        if (1) {
            $t = {%$common};
            $t->{test} = "write";
            $t->{startIOSec} = $startIOSec;
            $t->{stopAfterSec} = $stopAfterSecWrite;    # measurement stops after this many seconds
            $t->{dataSizeMB} = $minWriteMB;             # write at least this many MB
            push(@testList, $t);
        }

        # Read
        foreach my $repeat_read (1..1) {
            $t = {%$common};
            $t->{test} = "read";
            $t->{startIOSec} = $startIOSec;
            $t->{stopAfterSec} = $stopAfterSecRead;    # measurement stops after this many seconds
            push(@testList, $t);
        }
    }
    else {
        # Standard TestDFSIO
        # Write
        foreach my $repeat_write (1..1) {
            $t = {%$common};
            $t->{test} = "write";
            $t->{dataSizeMB} = $minWriteMB;
            push(@testList, $t);
        }

        # Read
        foreach my $repeat_read (1..1) {
            $t = {%$common};
            $t->{test} = "read";
            $t->{dataSizeMB} = $minWriteMB;
            push(@testList, $t);
        }
    }
}}}}}}}}}}}

# Print test list in Perl format.
local $Data::Dumper::Indent = 3;
print(Data::Dumper->Dump([\@testList], ['*testList']));

print STDERR "Number of tests generated: " . scalar(@testList) . "\n";

