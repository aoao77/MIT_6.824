#!/usr/local/bin/python3
# from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import getopt
import itertools
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

# total 次数 @300
# tests 命令 @2C
# iterations 迭代次数 1000
# workers 进程数 @30
# output 输出目录 ./output
# race 竞争 
def run_test(test: str, race: bool, verbose: int):
    test_cmd = ["go", "test", f"-run=2{test}"]
    if race:
        test_cmd.append("-race")
    f, path = tempfile.mkstemp()
    start = time.time()
    proc = subprocess.run(test_cmd, stdout=f, stderr=f)
    runtime = time.time() - start
    os.close(f)
    return test, path, proc.returncode, runtime

# This will collate tests, so they run in a balanced way

def main(argv):
    # define args
    total = 100
    completed = 0
    tests = ''
    iterations = 1000
    workers = 25
    output = "./output"
    race = False
    verbose = 0

    try:
        opts, args = getopt.getopt(argv, "ht:c:w:v:r")
    except getopt.GetoptError:
        print('Error:  -t <times> -c <command> -w <workers> -v <verbose> -r <race> ')
        sys.exit(2)

    # 处理 返回值options是以元组为元素的列表。
    for opt, arg in opts:
        if opt in ("-h"):
            print(' -t <times> -c <command> -w <workers> -v <verbose> -r <race> ')
            sys.exit()
        elif opt in ("-t"):
            total = int(arg)
        elif opt in ("-c"):
            tests = arg
        elif opt in ("-w"):
            workers = int(arg)        
        elif opt in ("-v"):
            verbose = arg
        elif opt in ("-r"):
            race = True

    tests = itertools.chain.from_iterable(itertools.repeat(tests, iterations))

    with ThreadPoolExecutor(max_workers=workers) as executor:

        futures = []
        while completed < total:
            n = len(futures)
            # If there are fewer futures than workers assign them
            if n < workers:
                for test in itertools.islice(tests, workers-n):
                    futures.append(executor.submit(run_test, test, race, verbose))

            # Wait until a task completes
            done, not_done = wait(futures, return_when=FIRST_COMPLETED)

            for future in done:
                test, path, rc, runtime = future.result()

                dest = pathlib.Path(output + "/" + f"{test}_{completed}.log").as_posix()
                # If the test failed, save the output for later analysis
                if rc != 0:
                    print(f"Failed test {test} - {dest}")
                    pathlib.Path(output).mkdir(exist_ok=True, parents=True)
                    shutil.copy(path, dest)

                os.remove(path)
                completed += 1
                futures = list(not_done)
            
            print("\r",f"{completed}/{total}",end="",flush=True)
        print()
        sys.exit()

if __name__ == "__main__":
    main(sys.argv[1:])