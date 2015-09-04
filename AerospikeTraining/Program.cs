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
    class Program
    {
		public const int MAX_RECORDS = 10000;
		public const int MAX_DAYS = 60;
		public const int BIG_EVENTS = 1000;
		public const int SMALL_EVENTS = 10;

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


                // Present options
                Console.WriteLine("What would you like to do:");
                Console.WriteLine("1> Generate data");
                Console.WriteLine("2> Simulate daily events");
                Console.WriteLine("3> Get a day range of events for a user");
                Console.WriteLine("0> Exit");
                Console.Write("\nSelect 1-3 and hit enter:");
				int feature = int.Parse(Console.ReadLine());

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
                        default:
                            Console.WriteLine("********** Invalid Selection ");
                            break;
                    }
                }
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
					Bin Events = new Bin("events", EventList);
					client.Put (null, EventKey, User, Day, Sequence, Events);
					DayRecords++;
					EventRecords += NoOfEvents;

				}
				UserRecords++;
				if (x % 10 == 0) {
					Console.WriteLine ("User records: " + UserRecords +" Day records: " + DayRecords  +" Event records: " + EventRecords);
				}
			}
		}
		public void AddEvent(string user, DateTime day, string EventString){
			List<Object> EventList = null;
			String DateString = day.ToString ("yyyy-MM-dd");
			String EventKeyString = user + ":" + DateString + ":" + 1;
			Key EventKey = new Key ("test", "user-events", EventKeyString);
			Record eventRecord = client.Get (null, EventKey, "events");
			if (eventRecord != null) {
				updatePolicy.generation = eventRecord.generation;
				EventList = (List<Object>)eventRecord.GetValue ("events");
				EventList.Add (EventString);
				client.Put (updatePolicy, EventKey, new Bin ("events", EventList));
			} else {	
				Bin User = new Bin ("user-id", user);
				Bin Day = new Bin ("day", DateString);
				Bin Sequence = new Bin ("sequence", 1);
				EventList = new List<Object> ();
				EventList.Add (EventString);
				client.Put (createPolicy, EventKey, User, Day, Sequence, new Bin ("events", EventList));
			}
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
			Console.WriteLine("\nEnter start day (1 is today, 365 is 1 year ago):");
			min = int.Parse(Console.ReadLine());
			Console.WriteLine("Enter end day:");
			max = int.Parse(Console.ReadLine());
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
			List<Object> events = (List<Object>) eventRecord.GetValue ("events");
			String result = eventRecord.GetString ("user-id") + ":" + eventRecord.GetString ("day") + ":" + eventRecord.GetInt ("sequence") + ":" + events.Count;
			return result;
		}
	}
}

