using System.Globalization;
using System.Reactive.Linq;
using Microsoft.Extensions.Configuration;

namespace BookMl
{
	internal class Program
	{
		private static async Task Main(string[] args)
		{
			

			using CancellationTokenSource cancellationTokenSource = new();
			Console.CancelKeyPress += (_, eventArgs) =>
			{
				eventArgs.Cancel = true;
				cancellationTokenSource.Cancel();
			};

		
				  




		

			HttpListener listener = new HttpListener();
			await listener.StartAsync(cancellationTokenSource.Token);
		}
	}
}
