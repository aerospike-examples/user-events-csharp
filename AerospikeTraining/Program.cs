/*******************************************************************************
 * Copyright 2012-2015 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections;
using Aerospike.Client;

namespace AerospikeTraining
{
	public class KeyClass 
	{
		public String name;
		public String user;
		public int sequence;
		public KeyClass(String n, String u, int s) {
			name = n;
			user = u;
			sequence = s;
		}
	};
    class Program
    {

		public const int MAX_RECORDS = 10000;
		public const int MAX_DAYS = 60;
		public const int BIG_EVENTS = 1000;
		public const int SMALL_EVENTS = 10;
		// We want to limit the number of records in one sequence so we don't overflow the size of a block (~128k by default)
		// We have set this value artificially low to enable testing. Depending on blocksize and size of the entry in the list
		// this number could be much higher in a real application.
		public const int MAX_RECORDS_PER_SEQUENCE = 100;

		public const string EVENT_STRING = "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111" +
		                                   "2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222" +
		                                   "3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333" +
		                                   "4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444" +
		                                   "5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555" +
		                                   "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666" +
		                                   "7777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777" +
		                                   "8888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888";

		private AerospikeClient client;
		private DateTime epoch;
		private WritePolicy updatePolicy;
		private WritePolicy createPolicy;
		public Program(AerospikeClient c)
		{
			this.client = c;
			this.epoch = DateTime.Now;
			this.updatePolicy = new WritePolicy ();
			this.updatePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
			this.createPolicy = new WritePolicy ();
			this.createPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			KeyClass k = new KeyClass("test", "aUser", 1234556);
			Key keyy = new Key ("test", "user-name", Value.Get(k));
			k = k;
		}
        static void Main(string[] args)
        {
            Console.WriteLine("User event data grouped by day\n");
			Program p = null;
            try
            {
                ClientPolicy policy = new ClientPolicy();

//				policy.user = "dbadmin";
//				policy.password = "au=money";
//				policy.failIfNotConnected = true;
//				p = new Program(new AerospikeClient(policy, "C-9a8d04af83.aerospike.io", 3200));
				p = new Program(new AerospikeClient(policy, "127.0.0.1", 3000));

				int feature;
				do {
					
	                // Present options
	                Console.WriteLine("What would you like to do:");
	                Console.WriteLine("1> Generate data");
	                Console.WriteLine("2> Simulate daily events");
	                Console.WriteLine("3> Get a day range of events for a user");
	                Console.WriteLine("0> Exit");
	                Console.Write("\nSelect 1-3 and hit enter:");
					feature = int.Parse(Console.ReadLine());

	                if (feature != 0)
	                {
	                    switch (feature)
	                    {
	                        case 1:
	                            Console.WriteLine("********** Generate Data");
	                            p.GenerateData();
	                            break;
	                        case 2:
								Console.WriteLine("********** Simulate daily events");
	                            p.SimulateDailyEvents();
	                            break;
	                        case 3:
								Console.WriteLine("********** Get a day range of events for a user");
	                            p.DayRangeForUser();
	                            break;
							case 0:
								Console.WriteLine("Goodbye");
								break;
	                        default:
	                            Console.WriteLine("********** Invalid Selection ");
	                            break;
	                    }
	                }
				} while (feature != 0);
            }
            catch (AerospikeException e)
            {
                Console.WriteLine("AerospikeException - Message: " + e.Message);
                Console.WriteLine("AerospikeException - StackTrace: " + e.StackTrace);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception - Message: " + e.Message);
                Console.WriteLine("Exception - StackTrace: " + e.StackTrace);
            }
            finally
            {
                if (p.client != null && p.client.Connected)
                {
                    // Close Aerospike server connection
                    p.client.Close();
                }
            }
            
        } //main

		public void GenerateData(){
			Random rnd1 = new Random();
			Random rnd2 = new Random();
			Random rnd3 = new Random();

			int UserRecords = 0;
			for (int x = 1; x <= MAX_RECORDS; x++){
				int DayRecords = 0;
				int EventRecords = 0;
				String UserKeyString = "User-" + x;
				Key UserKey = new Key ("test", "user-events", UserKeyString);
				client.Put(null, UserKey, new Bin("user-id", UserKeyString));

				int NoOfEvents = 0; // number of events per day
				int DayPercent = 0; 
				if (x % 893 == 0) { // ocasionally write a large number of events per day
					NoOfEvents = rnd2.Next(500, BIG_EVENTS);
					DayPercent = rnd1.Next(80, 100);
				} else {
					NoOfEvents = rnd3.Next(1, SMALL_EVENTS);	
					DayPercent = rnd1.Next(1, 100);
				}

				int inc = Math.Max(MAX_DAYS / (100 - DayPercent), 1); // calculate how many days to write events

				for (int y = 1; y <= MAX_DAYS; y += inc) {
					DateTime EventDay = epoch.Subtract(TimeSpan.FromDays(y));
					String DateString = EventDay.ToString ("yyyy-MM-dd");
					String EventKeyString = UserKeyString + ":" + DateString + ":" + 1;
					Key EventKey = new Key ("test", "user-events", EventKeyString);
					Bin User = new Bin ("user-id", UserKeyString);
					Bin Day = new Bin("day", DateString);
					Bin Sequence = new Bin("sequence", 1);
					List<string> EventList = new List<string> ();
					for (int z = 1; z <= NoOfEvents; z++) {
						EventList.Add (z + ":" + EVENT_STRING);
					}
					Bin recordCount = new Bin ("total-records", NoOfEvents);
					Bin Events = new Bin("events", EventList);
					client.Put (null, EventKey, User, Day, Sequence, recordCount, Events);
					DayRecords++;
					EventRecords += NoOfEvents;

				}
				UserRecords++;
				if (x % 10 == 0) {
					Console.WriteLine ("User records: " + UserRecords +" Day records: " + DayRecords  +" Event records: " + EventRecords);
				}
			}
		}
		/*
		 * Return the one-based sequence number for a record, assuming a maximum of MAX_RECORDS_PER_SEQUENCE
		 * records will fit into one sequence record.
		 */
		private int GetSequenceNumberFromRecordNumber(int recordCount) {
			return (recordCount / MAX_RECORDS_PER_SEQUENCE) + 1;
		}

		private String FormKeyString(String user, String date, int sequenceNumber) {
			return user + ":" + date + ":" + sequenceNumber;
		}

		/*
		 * Add an event to the specified user on the specified day. If there
		 * are no events for this user on that day, a new user record will be created.
		 */
		public void AddEvent(string user, DateTime day, string EventString){
			List<Object> EventList = null;
			String DateString = day.ToString ("yyyy-MM-dd");
			String EventKeyString = FormKeyString(user, DateString, 1);
			Key EventKey = new Key ("test", "user-events", EventKeyString);
			Record eventRecord = client.Get (null, EventKey, "events", "total-records");
			if (eventRecord != null) {
				// Add a new event with a generation count for optimistic concurrency
				int currentCount = eventRecord.GetInt("total-records");
				if (currentCount < 0) {
					// Another thread has this record locked. Either retry or throw an exception
					throw new AerospikeException ("Record locked");
				}
				updatePolicy.generation = eventRecord.generation;
				// Determine the sequence number to write this record to
				int sequenceNumber = GetSequenceNumberFromRecordNumber(currentCount);
				if (sequenceNumber == 1) {
					// No need to lock, write to the base record
					EventList = (List<Object>)eventRecord.GetValue ("events");
					EventList.Add (EventString);
					Bin recordsCount = new Bin ("total-records", currentCount + 1);
					client.Put (updatePolicy, EventKey, recordsCount, new Bin ("events", EventList));

				} else {
					// we need to write a lock to the "head" (sequence 1) record, then write
					// the data, then finally unlock the head record and correct it's count
					client.Put (updatePolicy, EventKey, new Bin ("total-records", -1));

					try {
						// Now write the data. In this case we can always do a replace.
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
						// Add the user, date and sequence number for clarity. This is not needed in production
						// systems as the base record (sequence 1) contains all this information
						Bin User = new Bin ("user-id", user);
						Bin Day = new Bin ("day", DateString);
						Bin Sequence = new Bin ("sequence", sequenceNumber);
						Bin events = new Bin ("events", eventSequenceList);

						WritePolicy sequencePolicy = new WritePolicy ();
						sequencePolicy.recordExistsAction = RecordExistsAction.REPLACE;
						client.Put (sequencePolicy, keyForSequence, User, Day, Sequence, events);

						// Finally, unlock the record and update the count on it.
						updatePolicy.generation = updatePolicy.generation+1;
						client.Put (updatePolicy, EventKey, new Bin ("total-records", currentCount + 1));
					}
					catch (Exception) {
						// An error occurred, release the lock
						client.Put (updatePolicy, EventKey, new Bin ("total-records", currentCount));
						throw;
					}
				}
			} else {	
				// Create a new event with CREATE_ONLY in case someone else has already created it.
				Bin User = new Bin ("user-id", user);
				Bin Day = new Bin ("day", DateString);
				Bin Sequence = new Bin ("sequence", 1);
				Bin TotalRecords = new Bin ("total-records", 1);
				EventList = new List<Object> ();
				EventList.Add (EventString);
				client.Put (createPolicy, EventKey, User, Day, Sequence, TotalRecords, new Bin ("events", EventList));
			}
			Console.WriteLine ("Wrote event: " + EventKeyString + ":" + EventString.Substring (0, 10) + "...");
		}
		public void SimulateDailyEvents(){

			// add some events today

			AddEvent ("User-105", epoch, EVENT_STRING);
			AddEvent ("User-105", epoch, EVENT_STRING);
			AddEvent ("User-105", epoch, EVENT_STRING);
			AddEvent ("User-105", epoch, EVENT_STRING);
			AddEvent ("User-105", epoch, EVENT_STRING);
			AddEvent ("User-105", epoch, EVENT_STRING);

			AddEvent ("User-115", epoch, EVENT_STRING);
			AddEvent ("User-115", epoch, EVENT_STRING);
			AddEvent ("User-115", epoch, EVENT_STRING);
			AddEvent ("User-115", epoch, EVENT_STRING);

			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);
			AddEvent ("User-125", epoch, EVENT_STRING);

			// add some events yesterday
			DateTime YesterdayDay = epoch.Subtract(TimeSpan.FromDays(1));
			AddEvent ("User-125", YesterdayDay, EVENT_STRING);
			AddEvent ("User-125", YesterdayDay, EVENT_STRING);
			AddEvent ("User-125", YesterdayDay, EVENT_STRING);
			AddEvent ("User-125", YesterdayDay, EVENT_STRING);

		}
		public void DayRangeForUser(){
			// Get min and max dates
			int min;
			int max;
			string user;
			Console.WriteLine("Enter user (User-1):");
			user = Console.ReadLine().Trim();
			Console.WriteLine("\nEnter start day (0 is today, 365 is 1 year ago):");
			min = int.Parse(Console.ReadLine());
			Console.WriteLine("Enter end day:");
			max = int.Parse(Console.ReadLine());
			if (min > max) {
				int temp = max;
				max = min;
				min = temp;
			}
			List<Key> KeyList = new List<Key> ();
			for (int y = min; y <= max; y ++) {
				DateTime EventDay = epoch.Subtract (TimeSpan.FromDays (y));
				String DateString = EventDay.ToString ("yyyy-MM-dd");
				String EventKeyString = user + ":" + DateString + ":" + 1;
				Key EventKey = new Key ("test", "user-events", EventKeyString);
				KeyList.Add (EventKey);
			}
			Key[] keys = KeyList.ToArray ();
			Record[] records = client.Get (null, keys);
			int count = 0;
			foreach (Record record in records) {
				if (record != null) {
					Console.WriteLine (EventToString (record));	
					count++;
				}
			}
			Console.WriteLine("Records found: " + count);
		}

		private string EventToString(Record eventRecord){
			// Note: If we want to get a list of all the records to iterate through rather than just the count,
			// we get the maximum sequence from the count, iterate through the keys from 2->Max Sequence (as we
			// already have record 1), form a list of keys then do a batch read. This code is provided for clarity
			String user = eventRecord.GetString("user-id");
			String dateStr = eventRecord.GetString ("day");
			List<Object> events = (List<Object>) eventRecord.GetValue ("events");
			int count = eventRecord.GetInt ("total-records");
			if (count < 0) {
				// Another thread has this record locked. Either retry or throw an exception
				throw new AerospikeException ("Record locked");
			}

			List<Key> keyList = new List<Key> ();
			for (int i = 2; i <= GetSequenceNumberFromRecordNumber (count); i++) {
				keyList.Add (new Key ("test", "user-events", FormKeyString (user, dateStr, i)));
			}
			Key[] keys = keyList.ToArray ();
			Record[] records = client.Get (null, keys);
			foreach (Record record in records) {
				if (record != null) {
					List<Object> theseEvents = (List<Object>)record.GetValue ("events");
					events.AddRange(theseEvents);
				}
			}
				
			String result = eventRecord.GetString ("user-id") + ":" + eventRecord.GetString ("day") + ":" + eventRecord.GetInt ("sequence") + ":" + events.Count;
			return result;
		}
	}
}

