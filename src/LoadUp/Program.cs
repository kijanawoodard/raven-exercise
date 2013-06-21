using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using CommandLine;
using Raven.Client;
using Raven.Client.Document;

namespace LoadUp
{
	class Program
	{
		private const int Quantity = 1000*1000;
		private static IDocumentStore _store;

		private static int _concurrent, _total = 0;

		private static int _successWrites, _successReads = 0;
		private static int _failureWrites, _failureReads = 0;

		private static int _percentageReads = 50;
		private static Random _decider;

		static void Main(string[] args)
		{
			_decider = new Random();
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

			Console.WriteLine("Starting - {0} requests / {1}% reads", requests, _percentageReads);

			var timer = new System.Timers.Timer(1000);
			timer.Elapsed += Report;
			timer.Start();

			var sw = Stopwatch.StartNew();
			Parallel.For(0, requests, Execute);
			sw.Stop();

			Console.WriteLine();
			Console.WriteLine("************************************************");
			Console.WriteLine("{0,10:n0} succeeded \n{1,10:n0} failed \n{2:n0} per second \n{7} minutes \nreads:  {3,10:n0} {4,10:n0} \nwrites: {5,10:n0} {6,10:n0}", 
								_successReads+_successWrites, 
								_failureReads+_failureWrites, 
								Math.Round(requests / sw.Elapsed.TotalSeconds), 
								_successReads,_failureReads,
								_successWrites,_failureWrites,
								Math.Round(sw.Elapsed.TotalMinutes, 2));
			Console.WriteLine("************************************************");
			Console.WriteLine();
		}

		static void Report(object o, ElapsedEventArgs args)
		{
			Console.WriteLine("c:{0,2} t:{1,10:n0}", _concurrent, _total);
		}

		static void Execute(int i)
		{
			Interlocked.Increment(ref _concurrent);
			Interlocked.Increment(ref _total);
			if (_decider.Next(1, 100) <= _percentageReads)
				Read();
			else
				Write();
			Interlocked.Decrement(ref _concurrent);
		}

		static void Read()
		{
			var id = new Random().Next(1, Quantity);
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

		static void Write()
		{
			var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
			var random = new Random();
			var id = random.Next(1, Quantity);
			var character = chars[random.Next(chars.Length)];
			
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
}
