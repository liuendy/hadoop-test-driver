#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;

my @testList = ();

# Kill any jobs before tests.
push(@testList, {
    test => "killalljobs",
});

my $numComputeNodes = 8;                       # used only for calculating the appropriate parameters below; must change cluster manually
my $protectionLevel = "3x";
my $dataAccessPattern = "streaming";

foreach my $repeat (1..3) {
foreach my $blockSizeMiB (512) {
foreach my $ioFileBufferSizeKiB (128) {

    my $dataSizeMB = 1000*1000;

    my $baseDirectory = "/benchmarks/";
    $baseDirectory .= "$dataAccessPattern-$protectionLevel";
    $baseDirectory .= "/hduser1/terasort";
    
    my $common = {
        baseDirectory => $baseDirectory,
        blockSizeMiB => $blockSizeMiB,
        dataAccessPattern => $dataAccessPattern,
        dataSizeMB => $dataSizeMB,
        ioFileBufferSize => $ioFileBufferSizeKiB*1024,
        mapMaxAttempts => 3,
        mapOutputCompressCodec => "org.apache.hadoop.io.compress.Lz4Codec",
        maxTestAttempts => 3,
        protectionLevel => $protectionLevel,
        reduceMaxAttempts => 3,
        };
        
    my $t;
    
    # Teragen
    foreach my $repeat_gen (1..1) {
    foreach my $mapCores_gen (0) {
    foreach my $mapMemoryMB_gen (32) {            # 
    foreach my $mapTasksPerNode (8) {            # best is 64 but last iteration should be lower to improve Terasort performance
        my $mapTasks = $numComputeNodes * $mapTasksPerNode;         
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
    foreach my $terasortOutputReplication (1) {
    foreach my $reduceTasks (30, 100, 300) {              # best is 100
        # Terasort
        foreach my $reduceMemoryMB (2048) {           # best is 2048
        foreach my $sortFactor (3000) {               # best is 3000
        foreach my $sortMiB (768) {                   # should be > block size to avoid extra spills
            $t = {%$common};
            $t->{test} = "terasort";
            $t->{reduceMemoryMB} = $reduceMemoryMB;
            $t->{reduceTasks} = $reduceTasks;
            $t->{sortFactor} = $sortFactor;
            $t->{sortMiB} = $sortMiB;
            $t->{terasortOutputReplication} = $terasortOutputReplication;
            push(@testList, $t);
        }}}

        # Teravalidate
        foreach my $repeat_val (1..1) {
        foreach my $mapCores_val (0) {
        foreach my $mapMemoryMB_val (2048) {            # best is 2048
        foreach my $ioFileBufferSizeKiB_val (128) {     # best is 128
            $t = {%$common};
            $t->{test} = "teravalidate";
            $t->{ioFileBufferSize} = $ioFileBufferSizeKiB_val*1024;
            $t->{mapCores} = $mapCores_val;
            $t->{mapMemoryMB} = $mapMemoryMB_val;
            $t->{mapTasks} = $reduceTasks;                # map tasks in validate will always equal reduce tasks from terasort
            $t->{terasortOutputReplication} = $terasortOutputReplication;
            push(@testList, $t);
        }}}}
    }}}      

    # Grep
    foreach my $repeat_grep (1..0) {
    foreach my $mapMemoryMB_grep (8192, 2048, 1024, 4096) {            # best is 2048
    foreach my $ioFileBufferSizeKiB_grep (128) {     # best is 128
        $t = {%$common};
        $t->{test} = "grep";
        $t->{mapMemoryMB} = $mapMemoryMB_grep;
        $t->{ioFileBufferSize} = $ioFileBufferSizeKiB_grep*1024;
        push(@testList, $t);
    }}}
}}}
    
# Print test list in Perl format.
local $Data::Dumper::Indent = 3;
print(Data::Dumper->Dump([\@testList], ['*testList']));

print STDERR "Number of tests generated: " . scalar(@testList) . "\n";

