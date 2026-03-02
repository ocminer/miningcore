using System.Numerics;
using Miningcore.Blockchain.Equihash.DaemonResponses;
using Miningcore.Extensions;
using Miningcore.Native;
using Miningcore.Stratum;
using Miningcore.Util;
using Contract = Miningcore.Contracts.Contract;

namespace Miningcore.Blockchain.Junocash;

public class JunocashJob
{
    public JunocashJob(EquihashBlockTemplate blockTemplate, string jobId, string randomXRealm)
    {
        Contract.RequiresNonNull(blockTemplate);
        Contract.Requires<ArgumentException>(!string.IsNullOrEmpty(jobId));

        BlockTemplate = blockTemplate;
        JobId = jobId;
        RandomXRealm = randomXRealm;
        Height = blockTemplate.Height;
        SeedHash = blockTemplate.RandomXSeedHash;

        // Compute network difficulty from target
        var target = BitsToTarget(blockTemplate.Bits);
        var targetTop64 = target >> 192;
        NetworkDifficulty = targetTop64 == 0
            ? (double)(BigInteger.One << 64)
            : (double)((BigInteger.One << 64) / targetTop64);

        // Build the 140-byte header blob
        headerBlob = BuildHeaderBlob(blockTemplate);
    }

    private readonly byte[] headerBlob;

    public EquihashBlockTemplate BlockTemplate { get; }
    public string JobId { get; }
    public string RandomXRealm { get; }
    public uint Height { get; }
    public string SeedHash { get; }
    public double NetworkDifficulty { get; }

    /// <summary>
    /// Build 140-byte header blob:
    /// version(4) + prevHash(32) + merkleRoot(32) + blockCommitmentsHash(32) + nTime(4) + nBits(4) + nonce(32 zeros)
    /// </summary>
    private static byte[] BuildHeaderBlob(EquihashBlockTemplate template)
    {
        var blob = new byte[JunocashConstants.HeaderSize];

        // version (4 bytes LE)
        BitConverter.GetBytes(template.Version).CopyTo(blob, 0);

        // previousblockhash (32 bytes, reversed from hex BE to LE)
        template.PreviousBlockhash.HexToReverseByteArray().CopyTo(blob, 4);

        // merkleroot (32 bytes, reversed)
        var merkleRoot = template.DefaultRoots?.MerkleRoot ?? "0000000000000000000000000000000000000000000000000000000000000000";
        merkleRoot.HexToReverseByteArray().CopyTo(blob, 36);

        // blockcommitmentshash (32 bytes, reversed)
        var blockCommitmentsHash = template.DefaultRoots?.BlockCommitmentsHash
            ?? template.FinalSaplingRootHash
            ?? "0000000000000000000000000000000000000000000000000000000000000000";
        blockCommitmentsHash.HexToReverseByteArray().CopyTo(blob, 68);

        // nTime (4 bytes LE)
        BitConverter.GetBytes(template.CurTime).CopyTo(blob, 100);

        // nBits (4 bytes LE)
        var bits = Convert.ToUInt32(template.Bits, 16);
        BitConverter.GetBytes(bits).CopyTo(blob, 104);

        // nonce area (32 bytes) stays zero - filled per worker

        return blob;
    }

    /// <summary>
    /// Prepare a job for a specific worker. Injects the worker's extraNonce at offset 112.
    /// Returns the blob hex and target hex for the stratum response.
    /// </summary>
    public void PrepareWorkerJob(JunocashWorkerJob workerJob, out string blob, out string target)
    {
        workerJob.Height = Height;
        workerJob.SeedHash = SeedHash;

        // Clone header and inject extraNonce1 at offset 112 (inside the 32-byte nonce field)
        // Nonce layout: [miner_nonce(4 bytes) @ 108] [extraNonce1(8 bytes) @ 112] [zeros(20 bytes)]
        var workerBlob = new byte[JunocashConstants.HeaderSize];
        Array.Copy(headerBlob, workerBlob, JunocashConstants.HeaderSize);

        var extraNonce1Bytes = workerJob.ExtraNonce1.HexToByteArray();
        extraNonce1Bytes.CopyTo(workerBlob, 112);

        blob = workerBlob.ToHexString();
        target = EncodeTarget(workerJob.Difficulty);
    }

    /// <summary>
    /// Process a share submitted by a miner.
    /// </summary>
    public (Share Share, string BlockHex) ProcessShare(
        string minerNonceHex, string extraNonce1Hex, string workerHash,
        StratumConnection worker)
    {
        Contract.Requires<ArgumentException>(!string.IsNullOrEmpty(minerNonceHex));
        Contract.Requires<ArgumentException>(!string.IsNullOrEmpty(workerHash));

        var context = worker.ContextAs<JunocashWorkerContext>();

        // Reconstruct the header with full nonce
        var header = new byte[JunocashConstants.HeaderSize];
        Array.Copy(headerBlob, header, JunocashConstants.HeaderSize);

        // Parse the miner's submitted nonce (32 bytes = 64 hex chars from offset 108 of header)
        var minerNonce = minerNonceHex.HexToByteArray();
        var extraNonce1Bytes = extraNonce1Hex.HexToByteArray();

        // Detect SRBMiner-style miners that zero out extraNonce1 (bytes 4-7 of nonce)
        // vs cpuminer/xmrig-style that preserve the full blob
        var zerosAt4to7 = minerNonce.Length >= 8 &&
            minerNonce[4] == 0 && minerNonce[5] == 0 && minerNonce[6] == 0 && minerNonce[7] == 0;

        if(zerosAt4to7)
        {
            // SRBMiner-style: restore extraNonce1 + use miner's first 4 bytes
            minerNonce.AsSpan(0, 4).CopyTo(header.AsSpan(JunocashConstants.NonceOffset));
            extraNonce1Bytes.CopyTo(header, 112);
            // Bytes 120-139 stay zero
        }
        else
        {
            // cpuminer/xmrig-style: use full 32-byte nonce as-is
            minerNonce.AsSpan(0, Math.Min(minerNonce.Length, JunocashConstants.NonceSize))
                .CopyTo(header.AsSpan(JunocashConstants.NonceOffset));
        }

        // Hash the header with RandomX
        Span<byte> hash = stackalloc byte[32];
        RandomX.CalculateHash(RandomXRealm, SeedHash, header, hash);

        // Verify miner's submitted hash matches
        var hashHex = hash.ToHexString();
        if(hashHex != workerHash)
            throw new StratumException(StratumError.MinusOne, "bad hash");

        // Calculate difficulty from hash bytes 24-31 as LE uint64
        // diff = (1 << 64) / top64
        var hashBytes = hash.ToArray();
        var top64 = BitConverter.ToUInt64(hashBytes, 24);
        var shareDiff = top64 == 0
            ? double.MaxValue
            : (double)((BigInteger.One << 64) / (BigInteger)top64);

        var stratumDifficulty = context.Difficulty;
        var ratio = shareDiff / stratumDifficulty;
        var isBlockCandidate = shareDiff >= NetworkDifficulty;

        // Check share meets worker's difficulty
        if(!isBlockCandidate && ratio < 0.99)
        {
            if(context.VarDiff?.LastUpdate != null && context.PreviousDifficulty.HasValue)
            {
                ratio = shareDiff / context.PreviousDifficulty.Value;

                if(ratio < 0.99)
                    throw new StratumException(StratumError.LowDifficultyShare, $"low difficulty share ({shareDiff})");

                stratumDifficulty = context.PreviousDifficulty.Value;
            }
            else
                throw new StratumException(StratumError.LowDifficultyShare, $"low difficulty share ({shareDiff})");
        }

        var result = new Share
        {
            BlockHeight = Height,
            Difficulty = stratumDifficulty,
        };

        string blockHex = null;

        if(isBlockCandidate)
        {
            // Block hash = hash reversed for display (BE)
            var blockHash = hash.ToArray();
            Array.Reverse(blockHash);

            result.IsBlockCandidate = true;
            result.BlockHash = blockHash.ToHexString();

            // Serialize the full block for submission
            blockHex = SerializeBlock(header, hashBytes);
        }

        return (result, blockHex);
    }

    /// <summary>
    /// Serialize the full block:
    /// header(140) + compactSize(32) + solutionHash(32) + compactSize(txCount) + coinbasetxn + transactions
    /// </summary>
    private string SerializeBlock(byte[] header, byte[] solutionHash)
    {
        using var stream = new MemoryStream();

        // Header (140 bytes)
        stream.Write(header, 0, JunocashConstants.HeaderSize);

        // nSolution = CompactSize(32) + 32-byte RandomX hash (raw LE bytes)
        stream.WriteByte(32); // CompactSize for 32 bytes
        stream.Write(solutionHash, 0, 32);

        // Transaction count
        var txCount = 1 + (BlockTemplate.Transactions?.Length ?? 0);
        WriteCompactSize(stream, (ulong)txCount);

        // Coinbase transaction
        var coinbaseTxData = BlockTemplate.CoinbaseTx.Data.HexToByteArray();
        stream.Write(coinbaseTxData, 0, coinbaseTxData.Length);

        // Other transactions
        if(BlockTemplate.Transactions != null)
        {
            foreach(var tx in BlockTemplate.Transactions)
            {
                var txData = tx.Data.HexToByteArray();
                stream.Write(txData, 0, txData.Length);
            }
        }

        return stream.ToArray().ToHexString();
    }

    private static void WriteCompactSize(Stream stream, ulong value)
    {
        if(value < 253)
        {
            stream.WriteByte((byte)value);
        }
        else if(value <= 0xFFFF)
        {
            stream.WriteByte(253);
            stream.WriteByte((byte)(value & 0xFF));
            stream.WriteByte((byte)((value >> 8) & 0xFF));
        }
        else if(value <= 0xFFFFFFFF)
        {
            stream.WriteByte(254);
            var bytes = BitConverter.GetBytes((uint)value);
            stream.Write(bytes, 0, 4);
        }
        else
        {
            stream.WriteByte(255);
            var bytes = BitConverter.GetBytes(value);
            stream.Write(bytes, 0, 8);
        }
    }

    /// <summary>
    /// Convert compact bits to full 256-bit target
    /// </summary>
    private static BigInteger BitsToTarget(string bitsHex)
    {
        var bits = Convert.ToUInt32(bitsHex, 16);
        var exp = (int)((bits >> 24) & 0xFF);
        var man = (BigInteger)(bits & 0x00FFFFFF);
        var shift = 8 * (exp - 3);
        return shift >= 0 ? man << shift : man >> (-shift);
    }

    /// <summary>
    /// Encode target for stratum (8-byte LE hex for XMRig protocol)
    /// </summary>
    private static string EncodeTarget(double difficulty)
    {
        // target = (1 << 64) / difficulty, as 8-byte LE hex
        var target64 = (ulong)((BigInteger.One << 64) / (BigInteger)(ulong)Math.Max(difficulty, 1));
        var targetBytes = BitConverter.GetBytes(target64);

        // For XMRig stratum, use compact 4 or 8 byte target
        // If top 4 bytes are zero, send 4 bytes; otherwise send 8
        if(targetBytes[4] == 0 && targetBytes[5] == 0 && targetBytes[6] == 0 && targetBytes[7] == 0)
            return targetBytes.AsSpan(0, 4).ToHexString();

        return targetBytes.ToHexString();
    }
}
