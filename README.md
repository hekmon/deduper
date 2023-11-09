# deduper

Analyse 2 paths on the same file system to found identical files and hard link them to save space.

## How it works

* Indexing: both paths will be analyzed and the structure of the directories tree and their corresponding inodes mapped in memory (files & directory)
* Then the structure of the A path will be walked and for each regular file, the mapped memory structure of path B will be analyzed to find potential candidates
  * First a list of all files in B having the exact same size of the A files analyzed will be compiled (empty files will be ignored)
  * Then this list will be pruned based on several criterias
    * Candidates in B that are already hardlinks of the reference A file will be removed from the list
    * Files that do not have the same inode metadata (ownership [uid, gid] and file mode) will be removed from the candidates list to avoid breaking potential current access to these files (as hardlinks share the same metadata by design)
      * Unless the `-force` flag is set, in that case candidates are kept (but will have their metadata changed once hardlinking is done)
    * For candidates that are still on the list, a SHA256 checksum will be performed to ensure they have indeed the same content as the reference file in A currently being processed
* For candidates that have passed all the tests and are still on the candidates list:
  * if the `-apply` flag has been set
    * They will be removed (in order to free their path)
    * Reffile in A will be hard linked to the path that the B candidate had, making it available once again but dedupped with A this time
  * if the `-apply` flag has not been set
    * A reporting will be printed of what would have been done (how saved) with the flag on

## Usage

```
Usage of ./deduper:
  -apply
    	By default deduper run in dry run mode: set this flag to actually apply changes
  -dirA string
    	Referential directory
  -dirB string
    	Second directory to compare dirA against
  -force
    	Dedup files that have the same content even if their inode metadata (ownership and mode) is not the same
  -workers int
    	Set the maximum numbers of workers (default 40)
```

### Example

TODO
