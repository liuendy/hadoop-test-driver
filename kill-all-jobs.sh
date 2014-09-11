#!/bin/sh

ssh $1 "mapred job -list | grep job_ | awk ' { system(\"mapred job -kill \" \$1) } '"

