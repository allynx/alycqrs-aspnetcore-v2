using SqlSugar;
using System.Threading.Tasks;

namespace AlyCqrs.Synhronizers
{
    public interface ISynhronizer
    {
        Task ExceuteAsync(string sql, object parameter);
        Task ExceuteAsync(string sql, params SugarParameter[] parameters);
    }
}
