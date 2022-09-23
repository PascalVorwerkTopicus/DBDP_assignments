import sys
import os
sys.path.append('Framework')
from map_reduce_lib import MapReduceLib

def mapper_function(key_value_item):
    file, line = key_value_item
    track_id, user_id, datetime = line.split(',')

    hour = datetime.split(' ')[1].split(':')[0]

    return [((user_id, hour), 1)]


def reducer_function(key_value_item):
    user_id, hour = key_value_item[0]
    occurances = key_value_item[1]

    return [(user_id, (hour, sum(occurances)))]


def mapper2(key_value_item):
    return key_value_item

def reducer2(key_value_item):
    user_id, occurances = key_value_item
    sorted(occurances, key=lambda x: [1], reverse=True)
    return (user_id, occurances[0])

if __name__ == "__main__":
    # For each user the hour of the day (s)he listened most often to songs.
    # Expected output: (FirstName, LastName, hourOfday, numberOfTimesListened to a song in that hour of the day)
    print("Start of Assesment 1.1 problem 2.")

    history_file = os.path.join("Assignment1.1", "dataset_small", "playhistory.csv")
    if not os.path.isfile(history_file):
        print('File `%s` not found.' % history_file)
        sys.exit(-1)
    
    people_file = os.path.join("Assignment1.1", "dataset_small", "people.csv")
    if not os.path.isfile(history_file):
        print('File `%s` not found.' % history_file)
        sys.exit(-1)

    # Read data into separate lines
    file_contents = MapReduceLib.read_files([history_file])
    file_contents.pop(0) # to remove the headers

    people_contents = MapReduceLib.read_files([people_file])
    people_contents.pop(0) # to remove the headers

    # Execute MapReduce job in parallel
    map_reduce = MapReduceLib.MapReduce(mapper_function, reducer_function, 8)
    map_red_result = map_reduce(file_contents, debug=True)

    map_reduce = MapReduceLib.MapReduce(mapper2, reducer2, 8)
    map_red_result = map_reduce(map_red_result, debug=True)

    user_dict = {}
    for user, data in map_red_result:
        user_dict[user] = data
    
    #loop over everyone in people file:
    print("The first 20 users and at what hour they listen to music the most:")
    for person in people_contents[:20]:
        try:
            personinfo = person[1]
            person_id, firstname, lastname = personinfo.split(",")[:3]
            hourofday, times = user_dict[person_id]
            print(f"\t{firstname} {lastname}, {hourofday}hr, {times}x")
        except KeyError:
            print("This person doesnt like music")
    
    