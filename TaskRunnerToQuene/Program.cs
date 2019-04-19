using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace TaskRunnerToQuene
{
    class Program
    {
        readonly BlockingCollection<Tuple<int, string>> _inputQueue = new BlockingCollection<Tuple<int, string>>();
        readonly BlockingCollection<int> _outputQueue = new BlockingCollection<int>();

        static void Main(string[] args)
        {
            Task.Factory.StartNew(() => new Program().Run());
            Console.ReadLine();
        }

        public void Run()
        {
            Task.Factory.StartNew(Watcher);
            int threadCount = 4;
            Task[] tasks = new Task[threadCount];

            for (int i = 0; i < threadCount; ++i)
            {
                int taskId = i; //need to keep trace the task complete?
                Task task = new Task(() => TaskRunner(taskId));
                tasks[i] = task;
                task.Start();
            }

            for (int i = 0; i < 10; ++i)
            {
                Console.WriteLine("Queueing work item {0}", i);
                _inputQueue.Add(new Tuple<int, string>(i, "File Name " + i));
                System.Threading.Thread.Sleep(50);
            }

            _inputQueue.CompleteAdding();
            Console.WriteLine("Stopping adding.");

            Task.WaitAll(tasks);
            Console.WriteLine("CompleteAdding.");

            _outputQueue.CompleteAdding();
            Console.WriteLine("Done.");
        }

        public void TaskRunner(int taskId)
        {
            Console.WriteLine("TaskId {0} is starting.", taskId);

            // call Process Data method
            foreach (var queue in _inputQueue.GetConsumingEnumerable())
            {
                Console.WriteLine($"TaskId {taskId} is processing jobId {queue.Item1} = File Name => {queue.Item2}");

                // Call the job - Process data
                System.IO.File.AppendAllText(@"D:\Temp\FileTest.txt", $"TaskId {taskId} is processing jobId {queue.Item1} = File Name => {queue.Item2}" + Environment.NewLine);
                System.Threading.Thread.Sleep(200);
                Console.WriteLine($"TaskId {taskId} Done jobId {queue.Item1} = File Name => {queue.Item2}");

                // add to watcher which job done
                _outputQueue.Add(queue.Item1);
            }
            Console.WriteLine("TaskId {0} is stopping.", taskId);
            System.IO.File.AppendAllText(@"D:\Temp\FileTest.txt", $"TaskId => {taskId} Task done" + Environment.NewLine);

        }

        public void Watcher()
        {
            Console.WriteLine("Watcher is starting.");

            foreach (var job in _outputQueue.GetConsumingEnumerable())
            {
                Console.WriteLine("Watcher is using item {0}", job);
            }
            Console.WriteLine("Watcher is finished.");
        }
    }
}