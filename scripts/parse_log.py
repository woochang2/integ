from re import findall
from glob import glob
import sys

SEC = "s"
MICRO = "µs"
MILLI = "ms"
NANO = "ns"
MiB = "MiB"
KiB = "KiB"
B = "B"


def write_performance(size_in_KiB: float, duration_in_milli: float) -> float:
    return size_in_KiB / (duration_in_milli * 1000)
    
def convert_to_MiB(size: float, unit: str) -> float:
    if unit == B:
        return size / (2 << 20)
    elif unit == KiB:
        return size / 1024
    elif unit == MiB:
        return size 
    else:
        raise ValueError('Invalid unit')

def convert_to_milli(duration: float, unit: str) -> float:
    if unit == SEC:
        return duration * 1_000
    elif unit == MILLI:
        return duration 
    elif unit == MICRO:
        return duration / 1_000
    elif unit == NANO:
        return duration / 1_000_000
    else:
        raise ValueError('Invalid unit')
    
    
def pairwise(it):
    it = iter(it)
    while True:
        try:
            yield next(it), next(it)
        except StopIteration:
            # no more elements in the iterator
            return

def _parse_trie_persist_latency(log):
    tmp = findall(r'size=(\d+(?:\.\d+)?)([MK]?i?B)\s+time="?(\d+(?:\.\d+)?)([mnµs]+)"?', log)
    
    result = []
    for line in tmp:
        size, size_unit, duration, time_unit = line
        size = convert_to_MiB(float(size), size_unit)
        duration = convert_to_milli(float(duration), time_unit)
        result.append((size, duration, write_performance(size*float(1024), duration)))
        
    return result

def _parse_latency(log):
    tmp = findall(r'number=(\d+)\s+hash=[\w\d\.]+\s+blocks=\d+\s+txs=(\d+)', log)
    tx_num = {int(block_no): int(tx) for block_no, tx in tmp}

    tmp = findall(r'number=(\d+)\s+total="?(\d+(?:\.\d+)?)([mnµs]+)"?\s+Execution="?(\d+(?:\.\d+)?)([mnµs]+)"?\s+TrieUpdate="?(\d+(?:\.\d+)?)([mnµs]+)"?\s+Validation="?(\d+(?:\.\d+)?)([mnµs]+)"?\s+BlockWrite="?(\d+(?:\.\d+)?)([mnµs]+)"?\s+OtherCommit="?(\d+(?:\.\d+)?)([mnµs]+)"?', log)
    
    result = []
    for line in tmp:
        it_line = iter(line)
        block_no = int(next(it_line))
        block_insertion_latencies = (convert_to_milli(float(duration), unit) for duration, unit in pairwise(it_line))
        result.append((block_no, tx_num[block_no], *block_insertion_latencies))
        
    return result

def result(log):
    result = ""
        
    if latency:= _parse_latency(log):
        result += "\n[Latency (block_no; txs; total (ms); execution (ms); trieUpdate (ms); validation (ms); blockWrite (ms); otherCommit (ms))]\n"
        for item in latency:
            result += " ".join([f"{field}" for field in item])
            result += "\n"
    if trie_persist_latency:= _parse_trie_persist_latency(log):
        result += "\n[Trie Persist Latency (size (MiB); duration (ms); write_performance (KiB/s))]\n"
        for item in trie_persist_latency:
            result += " ".join([f"{field}" for field in item])
            result += "\n"
            
    return result

def process(target_file):
    assert isinstance(target_file, str)
    
    for filename in sorted(glob(target_file)):
        log = ""
        with open(filename, 'r') as f:
            log = f.read()
    
        with open(filename.split()[0]+".out", 'w') as f:
            f.write(result(log))
            
if __name__ == "__main__":
    target_file = sys.argv[1]
    process(target_file=target_file)
    
