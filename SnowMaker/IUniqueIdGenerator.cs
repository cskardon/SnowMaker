using System.Threading.Tasks;

namespace SnowMaker
{
    public interface IUniqueIdGenerator
    {
        //long NextId(string scopeName);
        long NextId(string scopeName, int? batchSize = null, int? prefetchWhenLeft = null);
    }
}