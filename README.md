# Using a List and a Sequence to store an unbounded collection of records
*Author: Tim Faulkes, Aerospike Senior Solutions Architect, Peter Milne, Aerospike Director of Applications Engineering*

Scenario: You need to store a collection of events associated with a particular user and a particular day. The number of events for a given day is typically small, but can in some cases be very large. The use of a large list (LLIST) is not suitable because XDR is desired or for performance reasons.

In the given use cases, the user wishes to be able to add events or query all events for a particular date range.

## Methodology
The list of events is stored in a bin associated with the user record. The key for the record is a composite key comprising ```user-id:date:sequence-number```. The sequence number starts at 1 and is created as such when the first record is created.

The record size is bounded by the block size of the SSD (assuming SSD/Flash is being used as the backing store), typically 128k but can be up to 1MB. The number of elements of a list that can be stored in the block depends on average element size but would typically be quite large. However, in this example for illustrative purposes we have assumed it will be limited to 100.

*Note: this approach assumes the average record size can be calculated and is a reasonable measure of typical records to be inserted. Counting the actual bytes in the list could be done with a similar approach if this assumption is not valid.*

##Reading the list
Given we assume that the number of elements that a block can hold is fixed, we can easily calculate the sequence number needed to hold a particular record:

```
		private int GetSequenceNumberFromRecordNumber(int recordCount) {
			return (recordCount / MAX_RECORDS_PER_SEQUENCE) + 1;
		}
```

This gives a one-based sequence number.

All records in the set will have an initial sequence of 1, so to query a user on a given date we can use a key of ```user:date:1```. This will give the initial block. If the initial block also holds the number of records in the list, the total number of sequences can be calculated.

For example, if the ```total-records``` bin of this initial sequence is 350 then by the above formula we can work there must be 4 sequences in the list being:
* user:dateStr1 -> Records 1 to 100
* user:dateStr:2 -> Records 101 to 200
* user:dateStr:3 -> Records 201 to 300
* user:dateStr:4 -> Records 301 to 350

So, to read all the elements in the list, we can get the number of records from the first block, form a list of keys for the other records and then do a batch read:
'''
	for (int i = 2; i <= GetSequenceNumberFromRecordNumber (count); i++) {
		keyList.Add(new Key ("test", "user-events", FormKeyString(user, dateStr, i)));	
		Key[] keys = keyList.ToArray ();
		Record[] records = client.Get (null, keys);
		foreach (Record record in records) {
			if (record != null) {
				List<Object> theseEvents = (List<Object>)record.GetValue ("events");
				events.AddRange(theseEvents);
			}
		}
	}
'''

Note that these individual sequences of records may now be stored on different nodes, so if the list was required to be processed using a UDF parts of this could now be done in parallel, resulting in performance improvements.

## Appending to the list
Writing elements to the list consists of an initial read of the first block (if it exists), then writing to the appropriate block and incrementing the ```total-records``` counter. 

If the number of elements dictates that the write will be in the first block, both these operations can be done immediately after the initial read.
```
	updatePolicy.generation = eventRecord.generation;
	// Determine the sequence number to write this record to
	int sequenceNumber = GetSequenceNumberFromRecordNumber(currentCount);
	if (sequenceNumber == 1) {
		// No need to lock, write to the base record
		EventList = (List<Object>)eventRecord.GetValue ("events");
		EventList.Add (EventString);
		Bin recordsCount = new Bin ("total-records", currentCount + 1);
		client.Put (updatePolicy, EventKey, recordsCount, new Bin ("events", EventList));
```

However, if the sequence number is greater than 1, we need to read the appropriate block, write the data and update the record count in this first block.

```
	Key keyForSequence = new Key("test", "user-events", FormKeyString (user, DateString, sequenceNumber));
	Record eventSequenceRecord = client.Get (null, keyForSequence, "events");
	List<Object> eventSequenceList;
	if (eventSequenceRecord == null || eventSequenceRecord.GetValue ("events") == null) {
		// This is a new record
		eventSequenceList = new List<Object> ();
	} else {
		eventSequenceList = (List<Object>)eventSequenceRecord.GetValue ("events");
	}
	eventSequenceList.Add (EventString);
	Bin events = new Bin ("events", eventSequenceList);
	WritePolicy sequencePolicy = new WritePolicy ();
	sequencePolicy.recordExistsAction = RecordExistsAction.REPLACE;
	client.Put (sequencePolicy, keyForSequence, events);
```

Since this requires multiple database operations we implement a primitive locking mechanism to ensure that multiple writers to the same node cannot affect each other. In this case we simply set the ```total-records``` count to -1, indicating that the record is locked and have readers and writers honour this value.

