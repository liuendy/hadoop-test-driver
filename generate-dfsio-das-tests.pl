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

foreach my $repeat (1..1) {
foreach my $numComputeNodes (10) {                  # must equal number of data nodes for DAS
foreach my $blockSizeMiB (128) {                    # no noticable effect
foreach my $ioFileBufferSizeKiB (128) {             # no noticable effect
foreach my $protectionLevel ("3x") {                # "21", "1", "2x", "3x"
foreach my $dataAccessPattern ("streaming") {
foreach my $mapTasksPerComputeNode (90, 8, 10) {    # 10, 39, 5, 18, 28

    my $nrFiles = int($mapTasksPerComputeNode * $numComputeNodes);

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
    my $maxReadMBPerSecPerComputeNode = 1500.0;
    my $actualReadSec = $startMeasurementSec + $stopAfterSecRead;  
    my $minWriteMB = $maxReadMBPerSecPerComputeNode * $actualReadSec * $numComputeNodes;
    
    my $baseDirectory = "/benchmarks/";
    $baseDirectory .= "$dataAccessPattern-$protectionLevel";
    $baseDirectory .= "/hduser1";

    my $common = {
        baseDirectory => $baseDirectory,
        blockSizeMiB => $blockSizeMiB,
        dataAccessPattern => $dataAccessPattern,
        ioFileBufferSize => $ioFileBufferSizeKiB*1024,
        mapMemoryMB => 32,      # Note that task will use a lot more virtual memory than this amount.
        maxTestAttempts => 3,
        nrFiles => $nrFiles,
        numComputeNodes => $numComputeNodes,
        numIsilonNodes => 0,
        protectionLevel => $protectionLevel,
        sortMiB => 4,
        startIOSec => $startIOSec,
        testVariant => 'com.emc.hadoop',
        };
        
    my $t;

    # Write (measurement only)
    foreach my $repeat_write (1..2) {
        $t = {%$common};
        $t->{test} = "write";
        $t->{stopAfterSec} = $stopAfterSecWrite;    # measurement stops after this many seconds
        push(@testList, $t);
    }
    
    # Write (measurement + prepare for read)
    $t = {%$common};
    $t->{test} = "write";
    $t->{stopAfterSec} = $stopAfterSecWrite;    # measurement stops after this many seconds
    $t->{dataSizeMB} = $minWriteMB;             # write at least this many MB
    push(@testList, $t);

    # Read
    foreach my $repeat_read (1..3) {
        $t = {%$common};
        $t->{test} = "read";
        $t->{stopAfterSec} = $stopAfterSecRead;    # measurement stops after this many seconds
        push(@testList, $t);
    }
}}}}}}}

# Print test list in Perl format.
local $Data::Dumper::Indent = 3;
print(Data::Dumper->Dump([\@testList], ['*testList']));

print STDERR "Number of tests generated: " . scalar(@testList) . "\n";

