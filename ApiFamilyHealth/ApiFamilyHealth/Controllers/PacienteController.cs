using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using ApiFamilyHealth.Models;

namespace TesteApi.Controllers
{
    public class PacienteController : ApiController
    {
        private readonly Paciente[] Pacientes = new Paciente[]
        {
            new Paciente {ID = 1, nome = "Michel Temer", cpf = "12345678911", email = "temer@gmail.com" },
            new Paciente {ID = 2, nome = "Luis Inácio", cpf = "00123400002", email = "lula@gmail.com" },
            new Paciente {ID = 3, nome = "Dilma Roussef", cpf = "00233344403", email = "dilma@gmail.com" },
            new Paciente {ID = 4, nome = "Aécio Neves", cpf = "12345678915", email = "aecio@gmail.com" }
        };

        // GET: api/Paciente
        public Paciente[] Get()
        {
            return Pacientes;
        }

        // GET: api/Paciente/5
        public Paciente Get(int id)
        {
            var pacientes = Pacientes;
            return pacientes.SingleOrDefault(x => x.ID == id);
        }

        // POST: api/Paciente
        public void Post([FromBody]string value)
        {
        }

        // PUT: api/Paciente/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/Paciente/5
        public void Delete(int id)
        {
        }
    }
}
