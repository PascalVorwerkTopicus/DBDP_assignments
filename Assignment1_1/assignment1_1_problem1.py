import sys
import os
sys.path.append('Framework')
sys.path.append('Assignment1_1')
from map_reduce_lib import MapReduceLib
from assignment1_1_utils import open_file_return_lines

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
    print("Start of Assesment 1_1 problem 1.")
    
    play_history_content = open_file_return_lines(os.path.join("Assignment1_1", "dataset_small", "playhistory.csv"), True)

    # Execute MapReduce job in parallel
    map_reduce = MapReduceLib.MapReduce(mapper_function, reducer_function, 8)
    map_red_result = map_reduce(play_history_content, debug=True)

    # Getting info out of result
    sorted_result = sorted(map_red_result, key=lambda x: (x[0]), reverse=True)
    sorted_played_result = sorted(map_red_result, key=lambda x: (-x[1]))

    print("SongId on alphabetic order played the following times:")
    for track_id, amount_played in sorted_result[:20]:
        print(f"\tSongId: {track_id}, played {amount_played} times in March 2015")

    print("\nTop 20 of most played songs in March 2015:")
    for track_id, amount_played in sorted_played_result[:20]:
        print(f"\tSongId: {track_id}, played {amount_played} times in March 2015")


