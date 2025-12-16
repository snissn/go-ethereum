#!/usr/bin/env python3
import sys
import re

def format_number(n):
    try:
        f = float(n)
        if f.is_integer():
            return "{:,}".format(int(f))
        return "{:,.2f}".format(f)
    except ValueError:
        return n

def format_time(val_str, unit_str):
    try:
        val = float(val_str)
        # Check unit to normalize if needed, but usually we just scale
        # Assuming input is usually ns/op from go test, but if it's already scaled?
        # Standard go test output is ns/op.
        if unit_str == "ns/op":
            if val >= 1_000_000_000:
                return "{:,.2f} s/op".format(val / 1_000_000_000)
            elif val >= 1_000_000:
                return "{:,.2f} ms/op".format(val / 1_000_000)
            elif val >= 1_000:
                return "{:,.2f} µs/op".format(val / 1_000)
            else:
                return "{:,.2f} ns/op".format(val)
        else:
            # Already scaled or unknown
            return f"{format_number(val_str)} {unit_str}"
    except ValueError:
        return f"{val_str} {unit_str}"

def parse_benchmark_line(line):
    # BenchmarkName 1000 100 ns/op ...
    parts = line.split()
    if len(parts) < 4:
        return None
    
    row = {}
    row['name'] = parts[0]
    row['iters'] = format_number(parts[1])
    
    # Time is usually the 3rd and 4th token (Value Unit)
    # We combine them for the cell value
    row['time'] = format_time(parts[2], parts[3])
    
    # Process remaining pairs
    i = 4
    while i < len(parts) - 1:
        val = parts[i]
        unit = parts[i+1]
        formatted_val = f"{format_number(val)} {unit}"
        
        if unit == "MB/s":
            row['throughput'] = formatted_val
        elif unit == "B/op":
            row['alloc_bytes'] = formatted_val
        elif unit == "allocs/op":
            row['alloc_count'] = formatted_val
        else:
            # Unknown column, just append to a generic list or ignore
            # For this specific table request, strictly typed columns are better.
            pass
        i += 2
        
    return row

def print_table(rows):
    if not rows:
        return

    # Define columns: key in dict, header (optional), alignment
    cols = [
        ('name', 'l'),
        ('iters', 'r'),
        ('time', 'r'),
        ('throughput', 'r'),
        ('alloc_bytes', 'r'),
        ('alloc_count', 'r')
    ]
    
    # Calculate widths
    widths = {}
    for key, align in cols:
        w = 0
        for r in rows:
            val = r.get(key, "")
            w = max(w, len(val))
        widths[key] = w

    # Print rows
    for r in rows:
        line_parts = []
        for i, (key, align) in enumerate(cols):
            val = r.get(key, "")
            width = widths[key]
            
            # If the column is empty for ALL rows, we might skip it?
            # But here we calculated width based on max. If max is 0, skip.
            if width == 0:
                continue
                
            if align == 'r':
                fmt = f"{{:>{width}}}"
            else:
                fmt = f"{{:<{width}}}"
            
            # Add some padding between columns (except the first)
            if i > 0:
                line_parts.append("  ") 
            
            line_parts.append(fmt.format(val))
        print("".join(line_parts))

def main():
    buffer = []
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            # Empty line: if we have a buffer, print it, then print empty line
            if buffer:
                print_table(buffer)
                buffer = []
            print()
            continue

        if line.startswith("Benchmark"):
            parsed = parse_benchmark_line(line)
            if parsed:
                buffer.append(parsed)
            else:
                # Failed to parse, treat as normal text
                if buffer:
                    print_table(buffer)
                    buffer = []
                print(line)
        else:
            # Non-benchmark line
            if buffer:
                print_table(buffer)
                buffer = []
            print(line)

    # Flush buffer at end
    if buffer:
        print_table(buffer)

if __name__ == "__main__":
    main()