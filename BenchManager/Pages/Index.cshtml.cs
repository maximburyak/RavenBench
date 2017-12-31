using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BenchManager.Pages
{    
    public class Client
    {
        public class Bench
        {
            public Task RequestTask;
            public string RelativeUrl;
            public string Template;
            public (string ParamName, string ParamValue)[] Params;

            public void ExecuteBench(string serverUrl)
            {
                var requestUrl = string.Join("/", serverUrl, Params.Select(x => x.ParamValue).ToArray());
                var req = WebRequest.Create(requestUrl);
                RequestTask = req.GetResponseAsync();                
            }
        }
        public string ServerUrl;
        public Bench[] Functions;
        public List<Bench> ActiveFunctions;

        public void ExecuteTask(Bench bench)
        {
            bench.ExecuteBench(ServerUrl);
            ActiveFunctions.Add(bench);
        }
    }
    public class IndexModel : PageModel
    {
        public List<Client> Servers = new List<Client>();
        public void OnGet()
        {

        }

        [BindProperty]
        public Client.Bench BenchOperation { get; set; }

        [BindProperty]
        public List<string> Params { get; set; }

        public async Task<IActionResult> OnPostAddTaskAsync(string clientUrl)
        {
            return RedirectToPage("/Index");
        }
    }
    
}
