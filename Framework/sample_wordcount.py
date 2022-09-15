#!/usr/bin/python3

import sys
import os.path
import string
from map_reduce_lib import *

def map_lines_to_words(key_value_item):
    """ Map function for the word count job. 
    Splits line into words, removes low information words (i.e. stopwords) and outputs (key, 1).
    """
    file, line = key_value_item
    process_print('is processing `%s` from %s' % (line, file))

    output = []
    
    # Convert to lowercase, trim and remove punctuation.
    line = line.lower().strip()
    line = line.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))

    # List with stopwords (low information words that should be removed from the string)
    STOP_WORDS = set([
            'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 
            'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
            ])

    # Split into words and add to output list
    for word in line.split():
        
        # Only if word is not in the stop word list, add to output
        if word not in STOP_WORDS:
            output.append( (word, 1) )
    
    return output

def reduce_word_count(key_value_item):
    """ Reduce function for the word count job. 
    Converts partitioned data (key, [value]) to a summary of form (key, value).
    """
    word, occurances = key_value_item
    return (word, sum(occurances))

if __name__ == '__main__':
    # Parse command line arguments
    if len(sys.argv) == 1:
        print('Please provide a text-file that you want to perform the wordcount on as a command line argument.')
        sys.exit(-1)
    elif not os.path.isfile(sys.argv[1]):
        print('File `%s` not found.' % sys.argv[1])
        sys.exit(-1)

    # Read data into separate lines
    file_contents = read_files([sys.argv[1]])
    
    # Execute MapReduce job in parallel
    map_reduce = MapReduce(map_lines_to_words, reduce_word_count, 8)
    word_counts = map_reduce(file_contents, debug=True)

    # Order the results in reverse order
    word_counts.reverse()
    top20 = word_counts[:20]
    
    print('Top 20 words by frequency:')
    for word, count in top20:
        print('{:10s}: {}'.format(word, count))