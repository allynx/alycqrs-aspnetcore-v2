using AlyCms.Commands.Sample;
using AlyCms.Domains.Sample;
using AlyCqrs.Commands;
using AlyCqrs.Storage;
using System;
using System.Threading.Tasks;

namespace AlyCms.CommandHandlers.Sample
{
    public class AbolishTesterCommandHandler : ICommandHandler<AbolishTesterCommand>
    {
        private readonly IRepository<Tester> _repository;

        public AbolishTesterCommandHandler(IRepository<Tester> repository)
        {
            _repository = repository;
        }
        public async Task ExecuteAsync(AbolishTesterCommand command)
        {
            if (command == null) { throw new ArgumentNullException("command", "The command is null!"); }
            Tester tester = await _repository.GetByKeyAsync(command.Id);
            tester.Abolish(tester.Id);
            await _repository.SaveAsync(tester);
        }
    }
}
