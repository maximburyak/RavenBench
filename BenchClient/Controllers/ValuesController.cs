using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace BenchClient.Controllers
{    
    public class ValuesController : Controller
    {
        // GET api/values

        [Route("GetTestList")]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/values
        [Route("Stuff")]
        public IEnumerable<string> GetStuff()
        {
            return new string[] { "stuff1", "stuff2" };
        }       
    }
}
