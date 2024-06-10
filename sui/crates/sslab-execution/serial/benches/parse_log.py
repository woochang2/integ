from re import findall
from glob import glob
import sys

MICRO = "µs"
MILLI = "ms"


def _parse_throughput(log):
    tmp = findall(r'thrpt:  \[\d+\.\d+ Kelem/s (\d+\.\d+) Kelem/s \d+\.\d+ Kelem/s\]', log)
    
    return [float(tps) for tps in tmp]
        

def _parse_latency(log):
    tmp = findall(r'Total: (\d+\.\d+), header_creation: \d+\.\d+, block_sealing: \d+\.\d+, execution: \d+\.\d+, persistence: \d+\.\d+', log)
    total = [float(s) for s in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, header_creation: (\d+\.\d+), block_sealing: \d+\.\d+, execution: \d+\.\d+, persistence: \d+\.\d+', log)
    header_creation = [float(s) for s in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, header_creation: \d+\.\d+, block_sealing: (\d+\.\d+), execution: \d+\.\d+, persistence: \d+\.\d+', log)
    sealing = [float(e) for e in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, header_creation: \d+\.\d+, block_sealing: \d+\.\d+, execution: (\d+\.\d+), persistence: \d+\.\d+', log)
    execution = [float(v) for v in tmp]
    
    tmp = findall(r'Total: \d+\.\d+, header_creation: \d+\.\d+, block_sealing: \d+\.\d+, execution: \d+\.\d+, persistence: (\d+\.\d+)', log)
    persist = [float(s) for s in tmp]
    
    tmp = findall(r'Ktps: (\d+\.\d+)', log)
    ktps = [float(t) for t in tmp]
    
    return ktps, total, header_creation, sealing, execution, persist

def result(log):
    result = ""
    
    if total := _parse_throughput(log):
        result = "[Throughput (ktps)]\n"
        for ktps in total:
            result += f"{ktps} \n"
        
        
    if latency:= _parse_latency(log):
        ktps, total, header_creation, sealing, execution, persist = latency
        result += "\n[Latency (Ktps; total (ms); header_creation (ms); sealing (ms); execution (ms); persist (ms))]\n"
        for k, t, h, s, e, p in zip(ktps, total, header_creation, sealing, execution, persist, strict=True):
            result += f"{k} {t} {h} {s} {e} {p}\n"
    
            
        
    return result

def process(target_file):
    assert isinstance(target_file, str)
    
    for filename in sorted(glob(target_file)):
        log = ""
        with open(filename, 'r') as f:
            log = f.read()
    
        with open(filename.split()[0]+".out", 'a') as f:
            f.write(result(log))
            
if __name__ == "__main__":
    target_file = sys.argv[1]
    process(target_file=target_file)