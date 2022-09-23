import sys
import os
sys.path.append('Framework')
from map_reduce_lib import MapReduceLib

def mapper_function(key_value_item):
    file, line = key_value_item
    track_id, user, datetime = line.split(',')

    if datetime.startswith("2015-03"):
        return [(track_id, 1)]
    else:
        return [(track_id, 0)]

def reducer_function(key_value_item):
    track_id, occurances = key_value_item
    return (track_id, sum(occurances))

if __name__ == "__main__":
    # For each song how often was listened to that song in a certain month of a particular year, i.e. March 2015. 
    # Expected output: (SongId, number of times played in March 2015), ordered by SongID
    print("Start of Assesment 1.1 problem 1.")
    
    file = os.path.join("Assignment1.1", "dataset_small", "playhistory.csv")
    if not os.path.isfile(file):
        print('File `%s` not found.' % file)
        sys.exit(-1)
    
    # Read data into separate lines
    file_contents = MapReduceLib.read_files([file])
    file_contents.pop(0) #to remove the headers

    # Execute MapReduce job in parallel
    map_reduce = MapReduceLib.MapReduce(mapper_function, reducer_function, 8)
    lol_result = map_reduce(file_contents, debug=True)

    sorted_result = sorted(lol_result, key=lambda x: (x[0]), reverse=True)
    sorted_played_result = sorted(lol_result, key=lambda x: (-x[1]))

    print("SongId on alphabetic order played the following times:")
    for track_id, amount_played in sorted_result[:20]:
        print(f"\tSongId: {track_id}, played {amount_played} times in March 2015")

    print("\nTop 20 of most played songs in March 2015:")
    for track_id, amount_played in sorted_played_result[:20]:
        print(f"\tSongId: {track_id}, played {amount_played} times in March 2015")


