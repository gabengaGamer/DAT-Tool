#==============================================================================
#
#  CDFSManager.py
#
#==============================================================================
#
#  CD File System Manager | CDFS
#
#==============================================================================

import os
import struct
import argparse
import time
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

#==============================================================================
#  DEFINES
#==============================================================================

CDFS_MAGIC = 0x43444653 #SFDC
CDFS_VERSION = 1

#==============================================================================

def unpack_string_from_table(string_table, offset):
    end = offset
    while end < len(string_table) and string_table[end] != 0:
        end += 1
    return string_table[offset:end].decode('utf-8')

#==============================================================================

def unpack_file_task(input_file, entry, file_path, file_offset, file_length):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(input_file, 'rb') as f:
        with open(file_path, 'wb') as out_file:
            f.seek(file_offset)
            remaining = file_length
            buffer_size = 16 * 1024 * 1024
            while remaining > 0:
                chunk_size = min(remaining, buffer_size)
                chunk = f.read(chunk_size)
                out_file.write(chunk)
                remaining -= chunk_size
    
    return file_path

#==============================================================================

def unpack_cdfs(input_file, output_dir, max_workers=None, debug_mode=False):        
    with open(input_file, 'rb') as f:
        header_data = f.read(40)
        magic, version, sector_size, recommended_cache_size, first_sector_offset, \
        total_sectors, file_table_length, file_table_entries, string_table_length, \
        string_table_entries = struct.unpack('<IIIIIIIIII', header_data)
        
        magic_text = struct.pack('>I', magic).decode('ascii')
        
        if magic != CDFS_MAGIC:
            print(f"Error: Invalid file format. Magic: {hex(magic)} | {magic_text}")
            return False
        
        if debug_mode: 
            print(f"CDFS Magic: {hex(magic)} | {magic_text}")
            print(f"CDFS Version: {version}")
            print(f"Sector Size: {sector_size} bytes")
            print(f"Recommended Cache Size: {recommended_cache_size} bytes")
            print(f"First Sector Offset: {first_sector_offset} bytes")
            print(f"Total Sectors: {total_sectors}")
            print(f"File Table Entries: {file_table_entries}")
        
        file_table = []
        for _ in range(file_table_entries):
            entry_data = f.read(16)
            file_name_offset, dir_name_offset, start_sector, length = struct.unpack('<IIII', entry_data)
            file_table.append({
                'file_name_offset': file_name_offset,
                'dir_name_offset': dir_name_offset,
                'start_sector': start_sector,
                'length': length
            })
        
        string_table_data = f.read(string_table_length)
        
        file_tasks = []
        for idx, entry in enumerate(file_table):
            file_name = unpack_string_from_table(string_table_data, entry['file_name_offset'])
            dir_name = unpack_string_from_table(string_table_data, entry['dir_name_offset'])
            
            file_offset = first_sector_offset + entry['start_sector'] * sector_size
            file_length = entry['length']
            
            if dir_name:
                dir_name = dir_name.replace('\\', '/')
                file_path = os.path.join(output_dir, dir_name, file_name)
            else:
                file_path = os.path.join(output_dir, file_name)
            
            file_tasks.append((entry, file_path, file_offset, file_length, idx))
    
    print(f"Unpacking {file_table_entries} files...")
    
    workers = max_workers or os.cpu_count()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_file = {}
        for entry, file_path, file_offset, file_length, idx in file_tasks:
            future = executor.submit(
                unpack_file_task, 
                input_file, 
                entry, 
                file_path, 
                file_offset, 
                file_length
            )
            future_to_file[future] = (file_path, idx, file_length)
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_file):
            file_path, idx, file_length = future_to_file[future]
            try:
                future.result()
                completed += 1
                if debug_mode:
                    print(f"unpacking [{completed}/{file_table_entries}]: {file_path} ({file_length} bytes)")
            except Exception as exc:
                print(f"Error unpacking {file_path}: {exc}")
    
    print(f"Unpacking completed. Files unpacked to {output_dir}")
    return True

#==============================================================================

def add_string_to_table(string_table, string_cache, string_value):
    string_value = string_value.upper()
    
    if string_value in string_cache:
        return string_cache[string_value]
    
    index = 0
    while index < (len(string_table) - len(string_value)):
        if (string_table[index:index+len(string_value)] == string_value.encode('utf-8') and 
            string_table[index+len(string_value)] == 0):
            string_cache[string_value] = index
            return index
        index += len(string_table[index:].split(b'\0', 1)[0]) + 1
    
    index = len(string_table)
    string_table += string_value.encode('utf-8') + b'\0'
    string_cache[string_value] = index
    return index

#==============================================================================

def process_file_task(idx, file_info, output_file, sector_size, first_sector_offset):
    try:
        with open(file_info['path'], 'rb') as in_file:
            file_data = in_file.read()
            
        file_offset = first_sector_offset + file_info['start_sector'] * sector_size
        sector_padding = (sector_size - (len(file_data) % sector_size)) % sector_size
        
        with open(output_file, 'r+b') as f:
            f.seek(file_offset)
            f.write(file_data)
            if sector_padding > 0:
                f.write(b'\0' * sector_padding)
        
        return (idx, len(file_data), file_info['path'])
    except Exception as e:
        return (idx, 0, str(e))

#==============================================================================

def pack_cdfs(input_dir, output_file, sector_size=2048, cache_size=128*1024, max_workers=None, debug_mode=False):      
   
    file_table = []
    file_paths = []
    
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, input_dir)
            file_size = os.path.getsize(file_path)
            
            file_paths.append({
                'path': file_path,
                'rel_path': rel_path,
                'size': file_size
            })
    
    print(f"Found {len(file_paths)} files for packing")
    
    string_table = bytearray(b'\0')
    string_table_entries = 1
    string_cache = {}
    
    for file_info in file_paths:
        rel_path = file_info['rel_path'].replace('/', '\\')
        dir_name, file_name = os.path.split(rel_path)
        
        dir_name_offset = add_string_to_table(string_table, string_cache, dir_name)
        file_name_offset = add_string_to_table(string_table, string_cache, file_name)
        
        file_table.append({
            'file_name_offset': file_name_offset,
            'dir_name_offset': dir_name_offset,
            'path': file_info['path'],
            'size': file_info['size']
        })
        
        if dir_name and dir_name_offset == len(string_table) - len(dir_name) - 1:
            string_table_entries += 1
        if file_name_offset == len(string_table) - len(file_name) - 1:
            string_table_entries += 1
    
    header_size = 40
    file_table_size = len(file_table) * 16
    string_table_size = len(string_table)
    first_sector_offset = header_size + file_table_size + string_table_size
    
    if first_sector_offset % sector_size != 0:
        padding = sector_size - (first_sector_offset % sector_size)
        first_sector_offset += padding
    else:
        padding = 0
    
    current_sector = 0
    for entry in file_table:
        entry['start_sector'] = current_sector
        sectors_needed = (entry['size'] + sector_size - 1) // sector_size
        current_sector += sectors_needed
    
    with open(output_file, 'wb') as f:
        header = struct.pack('<IIIIIIIIII', 
                             CDFS_MAGIC,
                             CDFS_VERSION,
                             sector_size,
                             cache_size,
                             first_sector_offset,
                             current_sector,
                             file_table_size,
                             len(file_table),
                             string_table_size,
                             string_table_entries)
        
        f.write(header)
        
        for entry in file_table:
            entry_data = struct.pack('<IIII',
                                    entry['file_name_offset'],
                                    entry['dir_name_offset'],
                                    entry['start_sector'],
                                    entry['size'])
            f.write(entry_data)
        
        f.write(string_table)
        
        if padding > 0:
            f.write(b'\0' * padding)
        
        total_size = sum(entry['size'] for entry in file_table)
        
        f.seek(first_sector_offset + current_sector * sector_size - 1)
        f.write(b'\0')
    
    workers = max_workers or os.cpu_count()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for idx, entry in enumerate(file_table):
            future = executor.submit(
                process_file_task,
                idx,
                entry,
                output_file,
                sector_size,
                first_sector_offset
            )
            futures.append(future)
        
        processed_size = 0
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            idx, file_size, file_path = future.result()
            completed += 1
            if isinstance(file_path, str) and os.path.exists(file_path):
                processed_size += file_size
                if debug_mode:
                    print(f"Packing [{completed}/{len(file_table)}]: {file_path} ({file_size} bytes)")
                    print(f"Progress: {processed_size}/{total_size} bytes ({int(processed_size*100/total_size)}%)")
            else:
                print(f"Error packing file {idx}: {file_path}")
    
    print(f"Packing completed. Archive saved to {output_file}")
    return True

#==============================================================================

def list_cdfs(input_file):
    with open(input_file, 'rb') as f:
        header_data = f.read(40)
        magic, version, sector_size, recommended_cache_size, first_sector_offset, \
        total_sectors, file_table_length, file_table_entries, string_table_length, \
        string_table_entries = struct.unpack('<IIIIIIIIII', header_data)
        
        if magic != CDFS_MAGIC:
            print(f"Error: Invalid file format. Magic number: {hex(magic)}")
            return False
            
        print(f"CDFS Version: {version}")
        print(f"File Count: {file_table_entries}")
        print(f"Sector Size: {sector_size} bytes")
        print(f"Total Archive Size: {os.path.getsize(input_file)} bytes")
        print(f"Total Sectors: {total_sectors}")
        print("\nContents:")
        print(f"{'Index':<6} {'Size':<12} {'Path'}")
        print("-" * 80)
        
        file_table = []
        for i in range(file_table_entries):
            entry_data = f.read(16)
            file_name_offset, dir_name_offset, start_sector, length = struct.unpack('<IIII', entry_data)
            file_table.append({
                'file_name_offset': file_name_offset,
                'dir_name_offset': dir_name_offset,
                'start_sector': start_sector,
                'length': length
            })
        
        string_table_data = f.read(string_table_length)
        
        for idx, entry in enumerate(file_table):
            file_name = unpack_string_from_table(string_table_data, entry['file_name_offset'])
            dir_name = unpack_string_from_table(string_table_data, entry['dir_name_offset'])
            
            if dir_name:
                full_path = f"{dir_name}\\{file_name}"
            else:
                full_path = file_name
                
            print(f"{idx:<6} {entry['length']:<12} {full_path}")
    
    return True

#==============================================================================

def verify_cdfs(input_file):
    try:
        with open(input_file, 'rb') as f:
            header_data = f.read(40)
            if len(header_data) < 40:
                print("Error: File is too small to be a valid CDFS archive")
                return False
                
            magic, version, sector_size, recommended_cache_size, first_sector_offset, \
            total_sectors, file_table_length, file_table_entries, string_table_length, \
            string_table_entries = struct.unpack('<IIIIIIIIII', header_data)
            
            if magic != CDFS_MAGIC:
                print(f"Error: Invalid file format. Magic number: {hex(magic)}")
                return False
                
            if version != CDFS_VERSION:
                print(f"Warning: Version mismatch. Expected {CDFS_VERSION}, found {version}")
            
            print("Verifying file table integrity...")
            
            file_size = os.path.getsize(input_file)
            if first_sector_offset > file_size:
                print(f"Error: First sector offset ({first_sector_offset}) exceeds file size ({file_size})")
                return False
            
            if file_table_entries * 16 != file_table_length:
                print(f"Error: File table length mismatch. Expected {file_table_entries * 16}, found {file_table_length}")
                return False
            
            file_table = []
            for i in range(file_table_entries):
                entry_data = f.read(16)
                if len(entry_data) < 16:
                    print(f"Error: Truncated file table at entry {i}")
                    return False
                    
                file_name_offset, dir_name_offset, start_sector, length = struct.unpack('<IIII', entry_data)
                file_table.append({
                    'file_name_offset': file_name_offset,
                    'dir_name_offset': dir_name_offset,
                    'start_sector': start_sector,
                    'length': length
                })
            
            print("Verifying string table integrity...")
            string_table_data = f.read(string_table_length)
            if len(string_table_data) < string_table_length:
                print(f"Error: Truncated string table")
                return False
            
            print("Verifying file entries and offsets...")
            for idx, entry in enumerate(file_table):
                file_offset = first_sector_offset + entry['start_sector'] * sector_size
                file_end = file_offset + entry['length']
                
                if file_end > file_size:
                    print(f"Error: File entry {idx} extends beyond end of archive")
                    return False
                
                if entry['file_name_offset'] >= string_table_length:
                    print(f"Error: File entry {idx} has invalid filename offset")
                    return False
                    
                if entry['dir_name_offset'] >= string_table_length:
                    print(f"Error: File entry {idx} has invalid directory name offset")
                    return False
                
                try:
                    file_name = unpack_string_from_table(string_table_data, entry['file_name_offset'])
                    dir_name = unpack_string_from_table(string_table_data, entry['dir_name_offset'])
                except UnicodeDecodeError:
                    print(f"Error: File entry {idx} has invalid string table references")
                    return False
            
            print("Verification successful!")
            print(f"Archive contains {file_table_entries} files across {total_sectors} sectors")
            return True
            
    except Exception as e:
        print(f"Error during verification: {str(e)}")
        return False

#==============================================================================

def print_help():
    print("")
    print("CDFS Manager (c)2001 Inevitable Entertainment Inc.")
    print("")
    print("Usage: CDFSManager <command> [options]")
    print("")
    print("Commands:")
    print("  pack <input_dir> <output_file> [--sector-size SIZE] [--cache-size SIZE] [--debug]")
    print("      Creates a CDFS archive from the specified directory.")
    print("")
    print("  unpack <input_file> <output_dir> [--debug]")
    print("      unpacks files from the specified CDFS archive to the given directory.")
    print("")
    print("  list <input_file>")
    print("      Lists the contents of the specified CDFS archive.")
    print("")
    print("  verify <input_file>")
    print("      Verifies the integrity of the specified CDFS archive.")
    print("")
    print("  help")
    print("      Displays this help text.")
    print("")
    print("Examples:")
    print("  CDFSManager pack my_folder output.dat")
    print("  CDFSManager unpack archive.dat my_folder")
    print("  CDFSManager list archive.dat")
    print("  CDFSManager verify archive.dat")

#==============================================================================

def print_command_help(command):
    if command == "pack":
        print("Usage: CDFSManager pack <input_dir> <output_file> [options]")
        print("")
        print("Creates a CDFS archive from the specified directory.")
        print("")
        print("Arguments:")
        print("  input_dir          Source directory containing files to pack")
        print("  output_file        Destination .dat file to create")
        print("")
        print("Options:")
        print("  --sector-size SIZE    Sets the sector size in bytes (default: 2048)")
        print("  --cache-size SIZE     Sets the recommended cache size in bytes (default: 131072)")
        print("  --debug               Display detailed information")
        print("")
        print("Example:")
        print("  CDFSManager pack my_folder output.dat")
    
    elif command == "unpack":
        print("Usage: CDFSManager unpack <input_file> <output_dir>")
        print("")
        print("unpacks files from the specified CDFS archive to the given directory.")
        print("")
        print("Arguments:")
        print("  input_file         Source .dat file to unpack")
        print("  output_dir         Destination directory for unpacked files")
        print("")
        print("Options:")
        print("  --debug            Display detailed information")
        print("")
        print("Example:")
        print("  CDFSManager unpack archive.dat my_folder")
    
    elif command == "list":
        print("Usage: CDFSManager list <input_file>")
        print("")
        print("Lists the contents of the specified CDFS archive.")
        print("")
        print("Arguments:")
        print("  input_file         .dat file to list contents of")
        print("")
        print("Example:")
        print("  CDFSManager list archive.dat")
    
    elif command == "verify":
        print("Usage: CDFSManager verify <input_file>")
        print("")
        print("Verifies the integrity of the specified CDFS archive.")
        print("")
        print("Arguments:")
        print("  input_file         .dat file to verify")
        print("")
        print("Example:")
        print("  CDFSManager verify archive.dat")
    
    else:
        print_help()

#==============================================================================

def main():
    if len(sys.argv) < 2:
        print_help()
        return

    command = sys.argv[1].lower()
    
    if command == "help" or command == "--help" or command == "-h":
        if len(sys.argv) > 2:
            print_command_help(sys.argv[2])
        else:
            print_help()
        return
    
    if command == "pack":
        if len(sys.argv) < 4:
            print_command_help("pack")
            return
            
        input_dir = sys.argv[2]
        output_file = sys.argv[3]
        sector_size = 2048
        cache_size = 128*1024
        dbg = False
        
        if not os.path.isdir(input_dir):
            print(f"Error: Directory {input_dir} doesn't exist")
            return False 
        
        i = 4
        while i < len(sys.argv):
            if sys.argv[i] == "--sector-size" and i+1 < len(sys.argv):
                sector_size = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--cache-size" and i+1 < len(sys.argv):
                cache_size = int(sys.argv[i+1])
                i += 2
            elif sys.argv[i] == "--debug":  
                dbg = True 
                i += 1                
            else:
                print(f"Unknown option: {sys.argv[i]}")
                return                            
        
        start_time = time.time()
        pack_cdfs(input_dir, output_file, sector_size, cache_size, debug_mode=dbg)
        if dbg:
            print(f"Time taken: {time.time() - start_time:.2f} seconds")
    
    elif command == "unpack":
        if len(sys.argv) < 4:
            print_command_help("unpack")
            return
            
        input_file = sys.argv[2]
        output_dir = sys.argv[3]       
        dbg = False     
        
        if not os.path.isfile(input_file):
            print(f"Error: File {input_file} doesn't exist")
            return  
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
               
        i = 4
        while i < len(sys.argv):
            if sys.argv[i] == "--debug":  
                dbg = True 
                i += 1                
            else:
                print(f"Unknown option: {sys.argv[i]}")
                return
        
        start_time = time.time()
        unpack_cdfs(input_file, output_dir, debug_mode=dbg)
        if dbg:
            print(f"Time taken: {time.time() - start_time:.2f} seconds")
    
    elif command == "list":
        if len(sys.argv) < 3:
            print_command_help("list")
            return
            
        input_file = sys.argv[2]
        
        if not os.path.isfile(input_file):
            print(f"Error: File {input_file} doesn't exist")
            return
        
        list_cdfs(input_file)
    
    elif command == "verify":
        if len(sys.argv) < 3:
            print_command_help("verify")
            return
            
        input_file = sys.argv[2]
        
        if not os.path.isfile(input_file):
            print(f"Error: File {input_file} doesn't exist")
            return
        
        verify_cdfs(input_file)
    
    else:
        print(f"Unknown command: {command}")
        print_help()

if __name__ == '__main__':
    main()