using System.Collections.Concurrent;

namespace Miningcore.Blockchain.Junocash;

public class JunocashWorkerJob
{
    public JunocashWorkerJob(string jobId, double difficulty)
    {
        Id = jobId;
        Difficulty = difficulty;
    }

    public string Id { get; }
    public ulong Height { get; set; }
    public string ExtraNonce1 { get; set; }
    public double Difficulty { get; set; }
    public string SeedHash { get; set; }

    public readonly ConcurrentDictionary<string, bool> Submissions = new(StringComparer.OrdinalIgnoreCase);
}
