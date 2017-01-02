#!/bin/bash

count=0
while read line
do
    # Declare an array containing all the elements on the line
    # About bash arrays: http://www.tldp.org/LDP/abs/html/arrays.html
    tokens=( $line )
    # The word (key) is the first element in the array
    new_word=${tokens[0]}
    # The number of occurrence of this word (value) is the second element in the array
    increment=${tokens[1]}
    # Remember that the key/value pairs produced by mappers
    # are sorted before being passed to the reducer.
    # Here we detect if the word in the key/value pair is the same
    # as at the previous iteration or if it has changed.
    # ${word+x} is a bash parameter expansion that is substituted to word if
    # and only if word is set (see https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html)
    if [[ ${new_word} = ${word} ]] || [ -z ${word+x} ]
    then
        # Word hasn't changed or it was not defined at the previous iteration:
        # increment the counter by the value in key/value pair.
	count=$((count+${increment}))
    else
        # Word has changed: emit the key/value pair and set the counter to the
        # value in key/value pair.
	echo $word $count
	count=${increment}
    fi
    # Save the word for next iteration    
    word=$new_word
done
# Emit last word
echo $word $count
