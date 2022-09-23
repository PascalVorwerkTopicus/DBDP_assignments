import sys
import os
sys.path.append('Framework')
from map_reduce_lib import MapReduceLib

def mapper_function(key_value_item):
    pass

def reducer_function(key_value_item):
    pass

if __name__ == "__main__":
    # For each user, the ar tist (s)he listen to most often. 
    # Expected output: (FirstName, LastName, Artist, NrofTimes listened to that artist) (Hint: you need a cascade of mappers and reducers. Explain why!)
    print("Start of Assesment 1.1 problem 4.")

