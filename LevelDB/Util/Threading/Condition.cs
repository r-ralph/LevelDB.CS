using System.Threading;

namespace LevelDB.Util.Threading
{
    public class Condition
    {
        private readonly object _lock = new object();

        public void AwaitUninterruptibly()
        {
            Monitor.Wait(_lock);
        }

        public void SignalAll()
        {
            Monitor.PulseAll(_lock);
        }
    }
}