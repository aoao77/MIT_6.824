#!/usr/local/bin/python3
# Mapping from topics to colors
# See the disclaimer at the end of the post if you
# want to use all RGB colors
import getopt
import os
import sys
from rich import print,columns

TOPICS = {
    "TIMR": "bright_black",
    "VOTE": "bright_cyan",
    "LEAD": "yellow",
    "TERM": "green",
    "LOG1": "blue",
    "LOG2": "cyan",
    "TRAN": "cyan",
    "CMIT": "magenta",
    "PERS": "white",
    "SNAP": "bright_blue",
    "DROP": "bright_red",
    "CLNT": "bright_green",
    "TEST": "bright_magenta",
    "INFO": "bright_white",
    "WARN": "bright_yellow",
    "ERRO": "red",
    "TRCE": "red",
    "APND": "white",
    "RECV":"bright_blue",
}
def main(argv):
    # define
    filename = "/home/gwm/yja/02_cs/01_MIT6.824/00_6.5840/src/raft/B_25.log"
    just = []
    ignore = []
    colorize = True
    n_columns = 5
    topics = [lvl for lvl in TOPICS.keys()]
    # input parameter
    try:
        opts, args = getopt.getopt(argv, "hf:c:j:i:")
    except getopt.GetoptError:
        print('Error:  -c <column> -j <just> -i <ignore> ')
        sys.exit(2)

    # 处理 返回值options是以元组为元素的列表。
    for opt, arg in opts:
        if opt in ("-h"):
            print(' -c <column> -j <just> -i <ignore>  ')
            sys.exit()
        elif opt in ("-c"):
            n_columns = int(arg)
        elif opt in ("-j"):
            just = arg.split(',')
        elif opt in ("-i"):
            ignore = arg.split(',')   
            print(ignore)
        elif opt in ("-f"):
            filename = arg
        
    # We can take input from a stdin (pipes) or from a file
    file = open(filename).readlines()
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics
    if just:
        print(f"just:{just}")
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]
        print(f"ignore:{ignore},topics:{topics}")

    topics = set(topics)
    # console = Console()
    width = os.get_terminal_size().columns
    print(width)
    panic = False
    for line in input_:
        try:
            space_split = line.split(" ")
            time_num = len(space_split[0])
            # Assume format from Go output
            time = int(line[:time_num])
            topic = line[time_num + 1:time_num + 5]
            msg = line[time_num + 6:].strip()
            # To ignore some topics
            if topic not in topics:
                continue

            # Debug() calls from the test suite aren't associated with
            # any particular peer. Otherwise we can treat second column
            # as peer id
            if topic != "TEST" and n_columns:
                i = int(msg[1])
                # msg = msg[3:]
            # Colorize output by using rich syntax when needed
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            # Single column. Always the case for debug calls in tests
            if n_columns is None or topic == "TEST":
                print(time, msg)
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = columns.Columns(cols, width=col_width - 1,
                            equal=True, expand=True)
                print(cols)
        except Exception as e:
            # Code from tests or panics does not follow format
            # so we print it as is
            # print(f"exception :{e}  line:{e.__traceback__.tb_lineno}")
            if line.startswith("panic"):
                panic = True
            # Output from tests is usually important so add a
            # horizontal line with hashes to make it more obvious
            if not panic:
                print("-" * width)
            print(line, end="")
        
if __name__ == "__main__":
    main(sys.argv[1:])