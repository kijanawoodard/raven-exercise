using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using CommandLine;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client;
using Raven.Client.Document;

namespace LoadUp
{
	class Program
	{
		private const int Quantity = 100*1000;
		private static IDocumentStore _store;

		private static int _concurrent, _total = 0;

		private static int _successWrites, _successReads = 0;
		private static int _failureWrites, _failureReads = 0;

		private static int _percentageReads = 50;
		private static int[] _decider;
		private static int[] _reads;
		private static int[] _writes;
		private static char[] _chars;

		const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

		static void Main(string[] args)
		{
			System.Net.ServicePointManager.DefaultConnectionLimit = 12 * 8; //8 cores

			_store = new DocumentStore
			{
				ConnectionStringName = "RavenDB",
			};
			_store.Initialize();

			var requests = 1;
			var options = new Options();
			if (CommandLine.Parser.Default.ParseArgumentsStrict(args, options))
			{
				requests = options.NumberOfRequests;
				_percentageReads = options.PercentageReads;
			}

			var rnd = new Random();
			_decider = Enumerable.Range(0, requests).Select(x => rnd.Next(1, 100)).ToArray();
			_reads = Enumerable.Range(0, requests).Select(x => rnd.Next(1, Quantity)).ToArray();
			_writes = Enumerable.Range(0, requests).Select(x => rnd.Next(1, Quantity)).ToArray();
			_chars = Enumerable.Range(0, requests).Select(x => rnd.Next(Chars.Length)).Select(x => Chars[x]).ToArray();

			Init();

			Console.WriteLine("Starting - {0} requests / {1}% reads", requests, _percentageReads);

			var timer = new System.Timers.Timer(1000);
			timer.Elapsed += Report;
			timer.Start();

			var sw = Stopwatch.StartNew();
			Parallel.For(0, requests, Execute);
			sw.Stop();

			using (new FlowerBox())
			{
				Console.WriteLine("{0,10:n0} succeeded \n" +
				                  "{1,10:n0} failed \n\n" +
				                  "{2:n0} per second \n" +
				                  "{7} minutes \nreads:  {3,10:n0} {4,10:n0} \n" +
				                  "writes: {5,10:n0} {6,10:n0} \n\n" +
				                  "final concurrent {8}", 
								_successReads+_successWrites, 
								_failureReads+_failureWrites, 
								Math.Round(requests / sw.Elapsed.TotalSeconds), 
								_successReads,_failureReads,
								_successWrites,_failureWrites,
								Math.Round(sw.Elapsed.TotalMinutes, 2),
								_concurrent);
			}
		}

		static void Report(object o, ElapsedEventArgs args)
		{
			Console.WriteLine("c:{0,2} t:{1,10:n0}", _concurrent, _total);
		}

		static void Execute(int i)
		{
			Interlocked.Increment(ref _concurrent);
			Interlocked.Increment(ref _total);
			if (_decider[i] <= _percentageReads)
				Read(i);
			else
				Write(i);
			Interlocked.Decrement(ref _concurrent);
		}

		static void Read(int i)
		{
			var id = _reads[i];
			using (var session = _store.OpenSession())
			{
				var document = session.Load<Document>(id);
				if (document == null)
				{
					Interlocked.Increment(ref _failureReads);
				}
				else
				{
					Interlocked.Increment(ref _successReads);
				}
			}
		}

		static void Write(int i)
		{
			var id = _writes[i];
			var character = _chars[i];
			
			using (var session = _store.OpenSession())
			{
				try
				{
					var document = session.Load<Document>(id);
					document.Data = new string(character, 2000);
					session.SaveChanges();
					Interlocked.Increment(ref _successWrites);
				}
				catch (Exception)
				{
					Interlocked.Increment(ref _failureWrites);
				}
			}
		}

		static void Init()
		{
			using (var session = _store.OpenSession())
			{
				var any = session.Query<Document>().Any();
				if (any)
				{
					using (new FlowerBox())
					{
						Console.WriteLine("Docs already in db");
					}
					
					return;
				}
			}

			using (new FlowerBox())
			{
				Console.WriteLine("Starting Init");	
			}

			var sw = Stopwatch.StartNew();
			var data = new string('a', 2000);
			var options = new BulkInsertOptions() { CheckForUpdates = true, BatchSize = (int)Math.Pow(2, 15) };

			using (var bulkInsert = _store.BulkInsert(options: options))
			{
				bulkInsert.Report += bulkInsert_Report;
				for (int i = 0; i < Quantity; i++)
				{
					bulkInsert.Store(new Document { Data = data });
				}
			}

			sw.Stop();

			using (new FlowerBox())
			{
				Console.WriteLine("Initialized {0:n0} documents in {1} minutes. {2} per second",
											Quantity,
											Math.Round(sw.Elapsed.TotalMinutes, 2),
											Math.Round(Quantity / sw.Elapsed.TotalSeconds));
			}
		}

		static void bulkInsert_Report(string obj)
		{
			Console.WriteLine(obj);
		}
	}

	class Document
	{
		public string Id { get; set; }
		public string Data { get; set; }
		public DateTimeOffset Updated { get; set; }

		public Document()
		{
			Updated = DateTimeOffset.UtcNow;
		}
	}

	public class Options
	{
		[Option('n', "requests", DefaultValue= 1000)]
		public int NumberOfRequests { get; set; }

		[Option('p', "percent-reads", DefaultValue = 50, HelpText = "Percentage of requests that will be reads. The remainder will be writes.")]
		public int PercentageReads { get; set; }
	}

	class FlowerBox : IDisposable
	{
		private readonly Action[] _graphics = new Action[]
		{
			() => Console.WriteLine(),
			() => Console.WriteLine("************************************************")
		};

		public FlowerBox()
		{
			_graphics.ForEach(action => action());
		}

		public void Dispose()
		{
			_graphics.Reverse().ForEach(action => action());
		}
	}
}
