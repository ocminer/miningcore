using System.Globalization;
using System.Numerics;
using System.Text.RegularExpressions;

namespace Miningcore.Blockchain.Junocash;

public static class JunocashConstants
{
    public const int HeaderSize = 140;
    public const int NonceOffset = 108;
    public const int NonceSize = 32;
    public const int ExtraNonce1Size = 8;
    public const int SolutionSize = 32;
    public const int SeedEpoch = 2048;
    public const int SeedLag = 96;

    public static readonly BigInteger Diff1b = BigInteger.Parse("00FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", NumberStyles.HexNumber);

    public static readonly Regex RegexValidNonce = new("^[0-9a-f]{64}$", RegexOptions.Compiled);

#if DEBUG
    public const int PayoutMinBlockConfirmations = 2;
#else
    public const int PayoutMinBlockConfirmations = 60;
#endif
}
