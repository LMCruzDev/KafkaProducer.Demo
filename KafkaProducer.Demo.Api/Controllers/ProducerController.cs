using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaProducer.Demo.Api.Model;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace KafkaProducer.Demo.Api.Controllers
{
    [Route("v1/producer")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerConfig config;

        public ProducerController(ProducerConfig config)
        {
            this.config = config;
        }

        [HttpPost]
        public async Task<IActionResult> Post(string topic, [FromBody]Employee employee)
        {
            var serializedEmployee = JsonConvert.SerializeObject(employee);
            
            using (var producer = new ProducerBuilder<Null,string>(config).Build())
            {
                await producer.ProduceAsync(topic, new Message<Null, string>
                {
                    Value = serializedEmployee
                });
                producer.Flush(TimeSpan.FromSeconds(10));
                return Ok(true);
            }
        }
    }
}
