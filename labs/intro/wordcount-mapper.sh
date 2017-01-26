#!/bin/bash

# Read all the lines passed on stdin
while read line
do
    # Split the words in the line
    for word in $line
    do
	# Emit a <word,1> key/value pair for every word
	echo $word 1
    done
done
