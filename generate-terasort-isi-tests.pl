#!/usr/bin/perl
#
# Usage:
#   $ ./generate-dfsio-tests.pl | ./hadoop-test-driver.pl phd21.cfg -
#

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
foreach my $numComputeNodes (11, 8, 6) {                  # should normally decrease                
foreach my $blockSizeMiB (512) {                    # best is 512
foreach my $hdfsThreads (256) {                     # no effect
foreach my $ioFileBufferSizeKiB (128) {             # no effect
foreach my $protectionLevel ("21") {
foreach my $dataAccessPattern ("streaming") {
foreach my $smartCache (1) {                        # best is 1 (enabled)

    my $dataSizeMB = 1000*1000;

    my $baseDirectory = "/benchmarks/";
    $baseDirectory .= "$dataAccessPattern-$protectionLevel";
    $baseDirectory .= "-0" if !$smartCache;
    $baseDirectory .= "/hduser1/terasort";
    
    my $common = {
        baseDirectory => $baseDirectory,
        blockSizeMiB => $blockSizeMiB,
        dataAccessPattern => $dataAccessPattern,
        dataSizeMB => $dataSizeMB,
        hdfsThreads => $hdfsThreads,
        ioFileBufferSize => $ioFileBufferSizeKiB*1024,
        mapOutputCompressCodec => "org.apache.hadoop.io.compress.Lz4Codec",
        maxTestAttempts => 3,
        numComputeNodes => $numComputeNodes,
        numIsilonNodes => $numIsilonNodes,
        protectionLevel => $protectionLevel,
        smartCache => $smartCache,
        };
        
    my $t;
    
    # Teragen
    foreach my $repeat_gen (1..1) {
    foreach my $mapCores_gen (0) {
    foreach my $mapMemoryMB_gen (32) {            # 
    foreach my $mapTasksPerNode (32, 16, 8) {
        my $mapTasks = $numIsilonNodes * $mapTasksPerNode;         
        $t = {%$common};
        $t->{test} = "teragen";    
        #$t->{skipIfHadoopFileFound} = "$baseDirectory/terasort-input/_SUCCESS";     # Skip teragen if we already have the data.
        $t->{mapCores} = $mapCores_gen;
        $t->{mapMemoryMB} = $mapMemoryMB_gen;
        $t->{mapTasks} = $mapTasks;
        push(@testList, $t);
    }}}}

    # Terasort + Teravalidate
    foreach my $repeat_sort (1..1) {
    foreach my $reduceTasks (30, 300, 100) {                     # best is 300 or 100, depending on compute node count
        # Terasort
        foreach my $reduceMemoryMB (2048) {                  # best is 2048
        foreach my $sortFactor (3000) {                 # best is 30 but 30-3000 have nearly the same result
        foreach my $sortMiB (768) {                          # should be > block size to avoid extra spills
            $t = {%$common};
            $t->{test} = "terasort";
            $t->{reduceMemoryMB} = $reduceMemoryMB;
            $t->{reduceTasks} = $reduceTasks;
            $t->{sortFactor} = $sortFactor;
            $t->{sortMiB} = $sortMiB;
            push(@testList, $t);
        }}}

        # Teravalidate
        foreach my $repeat_val (1..1) {
        foreach my $mapCores_val (0) {
        foreach my $mapMemoryMB_val (2048) {            # best is 2048
        foreach my $ioFileBufferSizeKiB_val (128) {     # best is 128
        foreach my $hdfsThreads_val ($hdfsThreads) {      # 24 and lower results in tasks timing out
            $t = {%$common};
            $t->{test} = "teravalidate";
            $t->{hdfsThreads} = $hdfsThreads_val;
            $t->{ioFileBufferSize} = $ioFileBufferSizeKiB_val*1024;
            $t->{mapCores} = $mapCores_val;
            $t->{mapMemoryMB} = $mapMemoryMB_val;
            $t->{mapTasks} = $reduceTasks;                # map tasks in validate will always equal reduce tasks from terasort
            push(@testList, $t);
        }}}}}
    }}    
}}}}}}}}}

# Print test list in Perl format.
local $Data::Dumper::Indent = 3;
print(Data::Dumper->Dump([\@testList], ['*testList']));

print STDERR "Number of tests generated: " . scalar(@testList) . "\n";

