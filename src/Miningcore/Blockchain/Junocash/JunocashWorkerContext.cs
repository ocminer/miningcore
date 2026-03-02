using Miningcore.Mining;

namespace Miningcore.Blockchain.Junocash;

public class JunocashWorkerContext : WorkerContextBase
{
    /// <summary>
    /// Usually a wallet address
    /// </summary>
    public override string Miner { get; set; }

    /// <summary>
    /// Arbitrary worker identifier for miners using multiple rigs
    /// </summary>
    public override string Worker { get; set; }

    /// <summary>
    /// Unique extra nonce assigned to this worker (hex string)
    /// </summary>
    public string ExtraNonce1 { get; set; }

    /// <summary>
    /// Current N job(s) assigned to this worker
    /// </summary>
    public Queue<JunocashWorkerJob> ValidJobs { get; private set; } = new();

    public void AddJob(JunocashWorkerJob job, int maxActiveJobs)
    {
        if(!ValidJobs.Contains(job))
            ValidJobs.Enqueue(job);

        while(ValidJobs.Count > maxActiveJobs)
            ValidJobs.Dequeue();
    }

    public JunocashWorkerJob GetJob(string jobId)
    {
        return ValidJobs.ToArray().FirstOrDefault(x => x.Id == jobId);
    }
}
