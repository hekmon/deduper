# deduper

Analyse 2 paths on the same file system to found identical files and hard link them to save space.

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
