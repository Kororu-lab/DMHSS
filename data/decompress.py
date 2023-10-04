import zstandard as zstd
import json
import csv
import os
import sys
import threading
from tqdm import tqdm
from queue import Queue

sys.set_int_max_str_digits(40000)

CHUNK_SIZE = 8192
BATCH_SIZE = 10000
NUM_THREADS = 8
FILE_BATCH_SIZE = 800000  # The threshold for segmenting files

file_counter = {}  # To keep track of file parts for each main file

def worker(queue, all_fields, comments_lock, output_file_base):
    comments = []
    while True:
        line = queue.get()
        if line is None:
            break

        try:
            comment = json.loads(line)
            with comments_lock:
                all_fields.update(comment.keys())
                comments.append(comment)

                if len(comments) >= BATCH_SIZE:
                    current_output_file = f"{output_file_base}_part{file_counter[output_file_base]}.csv"
                    write_to_csv(current_output_file, comments, all_fields, mode='a')
                    comments.clear()
                    if os.path.getsize(current_output_file) > FILE_BATCH_SIZE:
                        file_counter[output_file_base] += 1
        except:
            pass
        finally:
            queue.task_done()

    if comments:
        current_output_file = f"{output_file_base}_part{file_counter[output_file_base]}.csv"
        write_to_csv(current_output_file, comments, all_fields, mode='a')

def write_to_csv(output_file_path, comments, all_fields, mode='w'):
    with open(output_file_path, mode, newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=list(all_fields))
        if mode == 'w' or (mode == 'a' and os.path.getsize(output_file_path) == 0):
            writer.writeheader()
        for comment in comments:
            writer.writerow(comment)

def decompress_to_temp_file(input_file_path, temp_file_path):
    with open(input_file_path, 'rb') as f, open(temp_file_path, 'w', encoding='utf-8') as tf:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            while True:
                chunk = reader.read(CHUNK_SIZE)
                if not chunk:
                    break
                tf.write(chunk.decode('utf-8'))

def process_temp_file(temp_file_path, output_file_base):
    all_fields = set()
    comments_lock = threading.Lock()
    queue = Queue()

    if output_file_base not in file_counter:
        file_counter[output_file_base] = 0

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(queue, all_fields, comments_lock, output_file_base))
        t.start()
        threads.append(t)

    with open(temp_file_path, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc="Processing"):
            queue.put(line.strip())

    for _ in range(NUM_THREADS):
        queue.put(None)

    for t in threads:
        t.join()

    os.remove(temp_file_path)  # Delete the temporary file after processing

input_dir = "compressed"
output_dir = "processed"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for filename in os.listdir(input_dir):
    if filename.endswith(".zst"):
        input_file_path = os.path.join(input_dir, filename)
        temp_file_path = os.path.join(output_dir, filename.replace(".zst", ".temp"))
        output_file_base = os.path.join(output_dir, filename.replace(".zst", ""))

        # Decompress to temporary file
        decompress_to_temp_file(input_file_path, temp_file_path)
        
        # Process temporary file
        process_temp_file(temp_file_path, output_file_base)
