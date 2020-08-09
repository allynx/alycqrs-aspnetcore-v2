using AlyCms.Commands.Sample;
using AlyCms.Domains.Sample;
using AlyCqrs.Commands;
using AlyCqrs.Storage;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AlyCms.CommandHandlers.Sample
{
    public class CreateTesterCommandHandler : ICommandHandler<CreateTesterCommand>
    {
        private readonly IRepository<Tester> _repository;

        public CreateTesterCommandHandler(IRepository<Tester> repository) {
            _repository = repository;
        }
        public async Task ExecuteAsync(CreateTesterCommand command)
        {
            if (command == null) { throw new ArgumentNullException("command", "The command is null!"); }
            Tester tester = new Tester(command.Title,command.Disable);
            await _repository.SaveAsync(tester);
        }
    }
}
