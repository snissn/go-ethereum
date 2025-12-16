#!/usr/bin/env python3
import sys

def format_number(n):
    try:
        f = float(n)
        if f.is_integer():
            return "{:,}".format(int(f))
        return "{:,.2f}".format(f)
    except ValueError:
        return n

def format_time(ns_str):
    try:
        val = float(ns_str)
        if val >= 1_000_000_000:
            return "{:,.2f} s/op".format(val / 1_000_000_000)
        elif val >= 1_000_000:
            return "{:,.2f} ms/op".format(val / 1_000_000)
        elif val >= 1_000:
            return "{:,.2f} µs/op".format(val / 1_000)
        else:
            return "{:,.2f} ns/op".format(val)
    except ValueError:
        return ns_str + " ns/op"

def process_line(line):
    if not line.startswith("Benchmark"):
        return line.rstrip()

    parts = line.split()
    # Basic validation: Name, Iterations, Time, Unit(ns/op)
    if len(parts) < 4:
        return line.rstrip()
    
    # parts[0]: Name
    # parts[1]: Iterations
    # parts[2]: Time
    # parts[3]: Unit (should be ns/op)

    out = [parts[0], format_number(parts[1])]
    
    if parts[3] == "ns/op":
        out.append(format_time(parts[2]))
    else:
        # Fallback if unit is not ns/op (unexpected for go test)
        out.append(parts[2])
        out.append(parts[3])

    i = 4
    while i < len(parts):
        val = parts[i]
        # Check if next part is a unit
        if i + 1 < len(parts):
            unit = parts[i+1]
            # Heuristic: units usually contain / or are known like B/op, MB/s
            if '/' in unit or unit in ["MB/s", "B/op", "allocs/op"]:
                out.append(f"{format_number(val)} {unit}")
                i += 2
                continue
        
        # If we didn't consume as value+unit, just append
        out.append(val)
        i += 1
            
    return "\t".join(out)

if __name__ == "__main__":
    for line in sys.stdin:
        print(process_line(line))
