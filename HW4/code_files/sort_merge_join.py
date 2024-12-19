import os
import sys
import heapq
import json
import pandas as pd

# NOTE: remove all print(None) in the template code before submission

# parameter file is an json file that contains input parameters 
# for sort-merge join
# Do not change this to faciliate the grading
PARAS_FILE = 'parameters.json'

# temp_dir is a folder for storing runs during join process
# note each run is stored as a file
# You are free to change this directory name
TEMP_DIR = "temp_runs"

# This function will read the documents in the given file, e.g., country.json
#  chunk by chunk, sort each chunk by the given attribute (attr_name),
#  and output the sorted chunk (run) into a file in the temp_dir directory
# The code will create the temp_dir directory if it does not exist
# The function will return a list of paths to the chunk files, 
# e.g., [temp_runs/country_run0.json, temp_runs/country_run1.json, ...]
# Note: for grading purpose, make sure you name the files exactly like above
# In your submission, DO NOT add code for removing the temp_runs folder!

def produce_runs(file_name, attr_name, chunksize, temp_dir=TEMP_DIR):
  
    paths = []
    
    ################  fill in the code (1)

    # Params for temp file name
    base_name = os.path.splitext(os.path.basename(file_name))[0]
    chunk_num = 0

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    with open(file_name, 'r') as input_file:
        while True:
            chunk = list()
            for _ in range(chunksize):
                line = input_file.readline().strip()
                if not line: break
                chunk.append(json.loads(line))

            if not chunk: break

            chunk.sort(key = lambda x : x[attr_name])

            output_file = os.path.join(temp_dir, f"{base_name}_run{chunk_num}.json")
            with open(output_file,'w') as out:
                for record in chunk:
                    out.write(json.dumps(record)+ '\n')
            
            paths.append(output_file)
            chunk_num += 1

    return paths

# This function will intitialize the heap for merging the runs
# Note that runs are files that contain runs. 
# These files are generated by produce_runs()
# rows/documents will be coming out from heap in the join key order
def init_heap(runs, join_key): 
    heap = []
    
    readers = [open(run, 'r') for run in runs]
    for i, reader in enumerate(readers):
        json_row = next(reader)

        if json_row:
            row = json.loads(json_row)
            heapq.heappush(heap, (row[join_key], i, row, reader))
            
    return heap

# Getting next document with the smallest join key value 
# It will also update the heap by pushing new document from the same chunk, 
#  if any, into the heap
def next_from_runs(heap, join_key):
    if (not heap):
        return
    
    key, i, row, reader = heapq.heappop(heap)
    
    next_json_row = next(reader, None)
    if (next_json_row):
        next_row = json.loads(next_json_row)
        heapq.heappush(heap, (next_row[join_key], i, next_row, reader))
    else: # done with this run/chunk
        reader.close()
    
    return row


# Getting the next document and all documents with the same join key value
# note nexts instead of next in the function name
def nexts_from_runs(heap, join_key):
    if (not heap):
        return 
 
    res = []
    
    current_row = next_from_runs(heap, join_key)
    res.append(current_row)
    
    while (heap):
        head_row = heap[0][2] # take a look without popping from heap
        
        if (head_row[join_key] == current_row[join_key]):
            ################  fill in the code (2)
            next_row = next_from_runs(heap, join_key)
            res.append(next_row)
        else:
            break
        
    return res
    
# Merge runs to find joining tuples, extract projection attribute values, 
#  and output to result file
def merge_runs(runs1, runs2, 
               join_key1, proj_attrs1,
               join_key2, proj_attrs2, 
               output_file):
    
    # initialize the heaps, one for merging runs for each relation
    heap1 = init_heap(runs1, join_key1)
    heap2 = init_heap(runs2, join_key2)
        
    with open(output_file, 'w') as output:
        # pop from both heaps
        # note all documents in nexts1 have the same join key value
        # the same is true for nexts2
        nexts1 = nexts_from_runs(heap1, join_key1)
        nexts2 = nexts_from_runs(heap2, join_key2)
   
        while nexts1 and nexts2:             
            #print("nexts1: ", nexts1)
            #print("nexts2: ", nexts2)
            
            key1 = nexts1[0][join_key1]
            key2 = nexts2[0][join_key2]
            
            #print(key1, key2)
                        
            if key1 == key2:
                #print('found matches')
                
                # produce joined tuples and output to the output file
                # in the required formats
                # see handout and attachement for details
                
                ################  fill in the code (3)
                for record1 in nexts1:
                    for record2 in nexts2:
                        projection1 = {name: record1[name] for name in proj_attrs1}
                        projection2 = {name: record2[name] for name in proj_attrs2}
                        output.write(json.dumps(projection1) + json.dumps(projection2) + '\n')

                # updating nexts1 and nexts2 for the next iteration
                ################  fill in the code (4)
                nexts1 = nexts_from_runs(heap1, join_key1)   
                nexts2 = nexts_from_runs(heap2, join_key2)                      
            elif key1 < key2: 
                ################  fill in the code (5)
                nexts1 = nexts_from_runs(heap1, join_key1)   
            else:
                ################  fill in the code (6)
                nexts2 = nexts_from_runs(heap2, join_key2)        
                

def sort_merge_join(file1, join_key1, proj_attrs1, 
                    file2, join_key2, proj_attrs2, 
                    chunksize, output_file):
    runs1 = produce_runs(file1, join_key1, chunksize)
    #print("runs1:", runs1)
    runs2 = produce_runs(file2, join_key2, chunksize)
    #print("runs2:", runs2)
            
    merge_runs(runs1, runs2, join_key1, proj_attrs1,
                               join_key2, proj_attrs2,
                               output_file)


if __name__ == "__main__":

    paras = json.load(open(PARAS_FILE))

    file1 = paras["file1"]
    join_key1 = paras["join_key1"] 
    proj_attrs1 = paras["proj_attrs1"]
    file2 = paras["file2"]
    join_key2 = paras["join_key2"] 
    proj_attrs2 = paras["proj_attrs2"]
    chunksize = paras["chunksize"] 
    output_file = paras["output_file"]

    sort_merge_join(file1, join_key1, proj_attrs1, 
                    file2, join_key2, proj_attrs2, 
                    chunksize, output_file)