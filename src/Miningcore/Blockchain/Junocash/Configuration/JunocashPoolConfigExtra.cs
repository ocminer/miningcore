using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Miningcore.Blockchain.Junocash.Configuration;

public class JunocashPoolConfigExtra
{
    /// <summary>
    /// z-addr holding pool funds - required for payment processing
    /// </summary>
    [JsonProperty("z-address")]
    public string ZAddress { get; set; }

    /// <summary>
    /// RandomX virtual machine bucket
    /// Defaults to poolId if not specified
    /// </summary>
    public string RandomXRealm { get; set; }

    /// <summary>
    /// Optional override value for RandomX VM Flags
    /// </summary>
    public JToken RandomXFlagsOverride { get; set; }

    /// <summary>
    /// Optional additive value for RandomX VM Flags
    /// </summary>
    public JToken RandomXFlagsAdd { get; set; }

    /// <summary>
    /// Optional value for number of RandomX VMs allocated per generation (new seed hash)
    /// Set to -1 to scale to number of cores
    /// </summary>
    public int RandomXVMCount { get; set; } = 1;

    /// <summary>
    /// Custom Arguments for getblocktemplate RPC
    /// </summary>
    public JToken GBTArgs { get; set; }
}
