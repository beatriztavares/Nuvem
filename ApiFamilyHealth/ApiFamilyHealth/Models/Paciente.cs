using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ApiFamilyHealth.Models
{
    public class Paciente
    {
        public int ID { get; set; }
        public string nome { get; set; }
        public string cpf { get; set; }
        public string email { get; set; }
    }
}