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
    public class ChangeTesterCommandHandler : ICommandHandler<ChangeTesterCommand>
    {
        private readonly IRepository<Tester> _repository;

        public ChangeTesterCommandHandler(IRepository<Tester> repository)
        {
            _repository = repository;
        }
        public async Task ExecuteAsync(ChangeTesterCommand command)
        {
            if (command == null) { throw new ArgumentNullException("command", "The command is null!"); }
            Tester tester = await _repository.GetByKeyAsync(command.Id);
            tester.Update(tester.Id, command.Title,command.Disable);
            await _repository.SaveAsync(tester);
        }
    }
}
