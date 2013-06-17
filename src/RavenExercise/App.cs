using System;
using System.Diagnostics;
using System.Web.Mvc;
using Raven.Abstractions.Data;

namespace RavenExercise
{
	public class Document
	{
		public string Id { get; set; }
		public string Data { get; set; }
		public DateTimeOffset Updated { get; set; }

		public Document()
		{
			Updated = DateTimeOffset.UtcNow;
		}
	}

    public class InitController : Controller
    {
	    public static int Quantity = 1000*1000;
        public ActionResult Index()
        {
	        var sw = Stopwatch.StartNew();
	        var data = new string('a', 2000);
	        var options = new BulkInsertOptions() {CheckForUpdates = true, BatchSize = 2048};
	        using (var bulkInsert = MvcApplication.DocumentStore.BulkInsert(options: options))
	        {
				for (int i = 0; i < Quantity; i++)
		        {
					bulkInsert.Store(new Document { Data = data });    
		        }
	        }

			sw.Stop();

	        var result = string.Format("Initialized {0:n0} documents in {1} minutes. {2} per second",
										Quantity,
										sw.Elapsed.TotalMinutes,
										Math.Round(Quantity / sw.Elapsed.TotalSeconds));
	        return Content(result);
        }
    }

	public class ReadController : Controller
	{
		public ActionResult Index()
		{
			var random = new Random().Next(1, InitController.Quantity);
			using (var session = MvcApplication.DocumentStore.OpenSession())
			{
				var document = session.Load<Document>(random);
				return Content("Loaded " + document.Id);
			}
		}
	}

	public class WriteController : Controller
	{
		public ActionResult Index()
		{
			var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
			var random = new Random();
			var id = random.Next(1, InitController.Quantity);
			var character = chars[random.Next(chars.Length)];
			using (var session = MvcApplication.DocumentStore.OpenSession())
			{
				var document = session.Load<Document>(id);
				document.Data = new string(character, 2000);
				session.SaveChanges();
				return Content("Wrote " + document.Id);
			}
		}
	}

	public class HomeController : Controller
	{
		public ActionResult Index()
		{
			return Content("routes are /init, /read, and /write");
		}
	}
}
