## Ingestion Script Documentation

This script automates processing directories and files in the local filesystem for upload to Hadoop Distributed File System (HDFS). It renames files and keeps track of completed directories to prevent redundant processing.

### Script Overview

1. **List Directories:** Identifies all directories starting with "group" in a designated local path.
2. **Filter Processed Directories:** Excludes directories already processed based on a tracking file.
3. **Rename and Upload Files:**
    - Renames files within the chosen directory by appending the current date and hour.
    - Uploads renamed files to a structured HDFS location.
4. **Record Processed Directory:** Adds the processed directory name to the tracking file to avoid re-processing.

### Required Libraries

* `os`: Provides functions for interacting with the operating system.
* `datetime`: Offers classes for working with dates and times.
* `pydoop.hdfs`: Enables interaction with HDFS.
* `re`: Allows the use of regular expressions (not used in this specific script).

### Directory Listing and Filtering

**Listing Directories**

```python
dirs = os.listdir('/home/itversity/itversity-material/retail_pipeline/data')
groups = [d for d in dirs if os.path.isdir(os.path.join('/home/itversity/itversity-material/retail_pipeline/data', d)) and d.startswith('group')]
print(groups)
```

**Reading Already Processed Directories**

```python
with open('/home/itversity/itversity-material/retail_pipeline/data/written_groups.txt', 'r') as file:
    skip_dir = file.read().splitlines()
```

**Filtering New Directories**

```python
filtered_dir = [group for group in groups if group not in skip_dir]
if filtered_dir:
    files = os.listdir(f"/home/itversity/itversity-material/retail_pipeline/data/{filtered_dir[0]}")
else:
    print("No new directories to process.")
```

### Renaming Files and Uploading to HDFS

**`rename_files_and_upload` Function**

```python
def rename_files_and_upload(local_directory, hdfs_directory):
    for file_name in files:
        # ... (code to rename and upload files)
```

This function iterates through files, renames them with timestamps, creates necessary HDFS directories, and uploads the renamed files.

### Recording Processed Directories

**`write_dirs_to_file` Function**

```python
def write_dirs_to_file(file_path, group):
    # ... (code to record processed directory)
```

This function adds a directory name to a tracking file if it's not already listed.

**Recording the Processed Directory**

```python
write_dirs_to_file('/home/itversity/itversity-material/retail_pipeline/data/written_groups.txt', filtered_dir[0])
```

### Detailed Explanation

The script follows these steps:

1. Lists all directories in the specified local path.
2. Filters out previously processed directories.
3. If new directories exist:
    - Renames files with timestamps within the chosen directory.
    - Uploads renamed files to an organized HDFS location.
4. Records the processed directory name to prevent future re-processing.

This approach ensures only new directories are processed, files are renamed with timestamps for organization, and uploads are structured within HDFS.
