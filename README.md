# FastDB

FastDB is an (persistent) in-memory key/value store in Go.

## Motivation

I wanted to be able to access my data as fast as possible without it being persisted to disk.  
In the past I've used several other key-value solutions but as a challenge to myself, tried if I could make  
something that was faster than the fastest I've used.

It is also important to know that memory access will always be faster than disk access,  
but it goes without saying that memory is more limited than disk storage.


## Semi-technical information

This is in fact a key-value store, working with buckets.  
(Buckets are a kind of a "box" in which you store key-values of the same kind.)

The "trick" behind this, is that the real key is a combination of bucket_key when  
the data is persisted to disk.

When you open the database, you can set the timer (in milliseconds) which will be the  
trigger to persist to disk. A value of 100 should be okay.  
That means there is a tiny risk that data from within the last 100 milliseconds isn't  
written to disk when there is a power failure.

If you want to minimize that risk, use a sync-time of 0.  
(but this will be slower!)

## How it works

### Set

The way to store things:
```
	err = store.Set(bucket, key, value)
```
bucket - string  
key - int  
value - []byte

So it is ideal for storing JSON, for example:
```
	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	recordData, _ := json.Marshal(record)

	err = store.Set("texts", record.ID, recordData)
```

### Get

The way to retrieve 1 record:
```
	value, ok := store.Get(bucket, key)
```
bucket - string  
key - int  
value - []byte

### GetAll

The way to retrieve all the data from one bucket:
```
	records, ok := store.GetAll(bucket)
```
bucket - string  
key - int  
records - map[int][]byte

### Info

To get information about the storage:

```
	info := store.Info()
```
Will show the number of buckets and the total of records.

### Del

The way to delete 1 record:
```
	 ok, err := store.Del(bucket, key)
```
bucket - string  
key - int  
ok - bool (true: key was found and deleted)

### Defrag

If overtime there are many deletions, the database could be compressed,  
so the next time you'll open and read the file, it will be faster.

```
	err := store.Defrag()
```
if there's an error, the original file will exist as a.bak file.


## Some simple figures

Done on my Macbook Pro M1.
```
created 100000 records in 62.726042ms
read 100000 records in 250ns
sort 100000 records by key in 23.126917ms
sort 100000 records by UUID in 41.55275ms

```
Benchmarks
```
goos: darwin
goarch: arm64
pkg: github.com/marcelloh/fastdb
Benchmark_Get_Memory_1000
Benchmark_Get_Memory_1000-8         	51353016	        23.30 ns/op	       0 B/op	       0 allocs/op
Benchmark_Set_File_NoSyncTime
Benchmark_Set_File_NoSyncTime-8     	     157	      7094052 ns/op	     265 B/op	       3 allocs/op
Benchmark_Set_Memory
Benchmark_Set_Memory-8              	 3586731	        297.6 ns/op	      93 B/op	       1 allocs/op
Benchmark_Set_File_WithSyncTime
Benchmark_Set_File_WithSyncTime-8   	  468963	         2407 ns/op	     209 B/op	       3 allocs/op
Benchmark_Get_File_1000
Benchmark_Get_File_1000-8           	44613194	        26.18 ns/op	       0 B/op	       0 allocs/op
```

## Example(s)

In the examples directory, you will find an example on how to sort the data.  

If more examples are needed, I will write them.