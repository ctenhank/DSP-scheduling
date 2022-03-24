from ast import arg
import numpy as np
import argparse
from pathlib import Path


def parse_file_v1(path: str) -> list:
    p = Path(path)
    data = np.array(p.read_text().split('\n'))
    ret = []
    for d in data:
        if str(d).isdigit():
            ret.append(d) 
    return ret
                
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str, default='./', help='The path of input directory')
    parser.add_argument('--output', type=str, default='./', help='The path of output directory')
    args = parser.parse_args()
    
    path = Path(args.input)
    files = []
    for p in path.rglob('*.txt'):
        if p.name == 'report.txt':
            continue
        print(f'Path: {str(p)}')
        data = parse_file_v1(str(p))
        if len(data) > 0:
            data = np.array(data, dtype=np.int32)
            print(f'Length: {len(data)}, Mean: {data.mean()}, Std: {data.std()}')
        print('-'  * 50)
    
