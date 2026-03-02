using System.Reactive;
using System.Reactive.Linq;
using Autofac;
using Miningcore.Blockchain.Bitcoin;
using Miningcore.Blockchain.Bitcoin.DaemonResponses;
using Miningcore.Blockchain.Equihash;
using Miningcore.Blockchain.Equihash.DaemonResponses;
using Miningcore.Blockchain.Junocash.Configuration;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.JsonRpc;
using Miningcore.Messaging;
using Miningcore.Mining;
using Miningcore.Native;
using Miningcore.Notifications.Messages;
using Miningcore.Rpc;
using Miningcore.Stratum;
using Miningcore.Time;
using Miningcore.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using Contract = Miningcore.Contracts.Contract;
using static Miningcore.Util.ActionUtils;

namespace Miningcore.Blockchain.Junocash;

public class JunocashJobManager : JobManagerBase<JunocashJob>
{
    public JunocashJobManager(
        IComponentContext ctx,
        IMasterClock clock,
        IMessageBus messageBus) :
        base(ctx, messageBus)
    {
        Contract.RequiresNonNull(ctx);
        Contract.RequiresNonNull(clock);
        Contract.RequiresNonNull(messageBus);

        this.clock = clock;
    }

    private readonly IMasterClock clock;
    private DaemonEndpointConfig[] daemonEndpoints;
    private RpcClient rpc;
    private EquihashCoinTemplate coin;
    private EquihashCoinTemplate.EquihashNetworkParams chainConfig;
    private JunocashPoolConfigExtra extraPoolConfig;
    private string randomXRealm;
    private RandomX.randomx_flags? randomXFlagsOverride;
    private RandomX.randomx_flags? randomXFlagsAdd;
    private string currentSeedHash;

    public IObservable<Unit> Blocks { get; private set; }
    public BlockchainStats BlockchainStats { get; } = new();
    public EquihashCoinTemplate Coin => coin;

    #region Configuration

    public override void Configure(PoolConfig pc, ClusterConfig cc)
    {
        Contract.RequiresNonNull(pc);
        Contract.RequiresNonNull(cc);

        logger = LogUtil.GetPoolScopedLogger(typeof(JunocashJobManager), pc);
        poolConfig = pc;
        clusterConfig = cc;

        extraPoolConfig = pc.Extra.SafeExtensionDataAs<JunocashPoolConfigExtra>();
        coin = pc.Template.As<EquihashCoinTemplate>();

        if(pc.EnableInternalStratum == true)
        {
            randomXRealm = !string.IsNullOrEmpty(extraPoolConfig?.RandomXRealm) ? extraPoolConfig.RandomXRealm : pc.Id;
            randomXFlagsOverride = MakeRandomXFlags(extraPoolConfig?.RandomXFlagsOverride);
            randomXFlagsAdd = MakeRandomXFlags(extraPoolConfig?.RandomXFlagsAdd);
        }

        // extract daemon endpoints
        daemonEndpoints = pc.Daemons
            .Where(x => string.IsNullOrEmpty(x.Category))
            .ToArray();

        ConfigureDaemons();
    }

    #endregion

    #region Daemon Communication

    private async Task<RpcResponse<EquihashBlockTemplate>> GetBlockTemplateAsync(CancellationToken ct)
    {
        var subsidyResponse = await rpc.ExecuteAsync<ZCashBlockSubsidy>(logger, BitcoinCommands.GetBlockSubsidy, ct);

        var result = await rpc.ExecuteAsync<EquihashBlockTemplate>(logger,
            BitcoinCommands.GetBlockTemplate, ct, extraPoolConfig?.GBTArgs ?? (object) GetBlockTemplateParams());

        if(subsidyResponse.Error == null && result.Error == null && result.Response != null)
            result.Response.Subsidy = subsidyResponse.Response;
        else if(subsidyResponse.Error?.Code != (int) BitcoinRPCErrorCode.RPC_METHOD_NOT_FOUND)
            result = new RpcResponse<EquihashBlockTemplate>(null, new JsonRpcError(-1, $"{BitcoinCommands.GetBlockSubsidy} failed", null));

        return result;
    }

    private static object GetBlockTemplateParams()
    {
        return new object[]
        {
            new
            {
                capabilities = new[] { "coinbasetxn", "workid", "coinbase/append" },
            }
        };
    }

    private async Task<bool> SubmitBlockAsync(Share share, string blockHex)
    {
        var batch = new[]
        {
            new RpcRequest(BitcoinCommands.SubmitBlock, new[] { blockHex }),
            new RpcRequest(BitcoinCommands.GetBlock, new[] { share.BlockHash })
        };

        var results = await rpc.ExecuteBatchAsync(logger, CancellationToken.None, batch);

        // Check submit result
        var submitResult = results[0];
        var submitError = submitResult.Error?.Message ??
            submitResult.Error?.Code.ToString(System.Globalization.CultureInfo.InvariantCulture) ??
            submitResult.Response?.ToString();

        if(!string.IsNullOrEmpty(submitError))
        {
            logger.Warn(() => $"Block {share.BlockHeight} submission failed with: {submitError}");
            messageBus.SendMessage(new AdminNotification("Block submission failed",
                $"Pool {poolConfig.Id} {(!string.IsNullOrEmpty(share.Source) ? $"[{share.Source.ToUpper()}] " : string.Empty)}failed to submit block {share.BlockHeight}: {submitError}"));
            return false;
        }

        // Verify block was accepted
        var acceptResult = results[1];
        var block = acceptResult.Response?.ToObject<Block>();
        var accepted = acceptResult.Error == null && block?.Hash == share.BlockHash;

        if(!accepted)
        {
            logger.Warn(() => $"Block {share.BlockHeight} submission failed because block was not found after submission");
            messageBus.SendMessage(new AdminNotification("Block submission failed",
                $"Pool {poolConfig.Id} block {share.BlockHeight} was not found after submission"));
        }

        return accepted;
    }

    #endregion

    #region RandomX Seed Management

    private void UpdateSeedHash(EquihashBlockTemplate blockTemplate)
    {
        if(string.IsNullOrEmpty(blockTemplate.RandomXSeedHash))
            return;

        if(currentSeedHash != blockTemplate.RandomXSeedHash)
        {
            logger.Info(() => $"Detected new seed hash {blockTemplate.RandomXSeedHash} starting @ height {blockTemplate.Height}");

            if(poolConfig.EnableInternalStratum == true)
            {
                RandomX.WithLock(() =>
                {
                    // delete old seed
                    if(currentSeedHash != null)
                        RandomX.DeleteSeed(randomXRealm, currentSeedHash);

                    // activate new one
                    currentSeedHash = blockTemplate.RandomXSeedHash;
                    RandomX.CreateSeed(randomXRealm, currentSeedHash, randomXFlagsOverride, randomXFlagsAdd,
                        extraPoolConfig?.RandomXVMCount ?? 1);
                });
            }
            else
                currentSeedHash = blockTemplate.RandomXSeedHash;
        }
    }

    private RandomX.randomx_flags? MakeRandomXFlags(JToken token)
    {
        if(token == null)
            return null;

        if(token.Type == JTokenType.Integer)
            return (RandomX.randomx_flags) token.Value<ulong>();

        if(token.Type == JTokenType.String)
        {
            RandomX.randomx_flags result = 0;
            var value = token.Value<string>();

            foreach(var flag in value.Split("|").Select(x => x.Trim()).Where(x => !string.IsNullOrEmpty(x)))
            {
                if(Enum.TryParse(typeof(RandomX.randomx_flags), flag, true, out var flagVal))
                    result |= (RandomX.randomx_flags) flagVal;
            }

            return result;
        }

        return null;
    }

    #endregion

    #region Job Updates

    protected async Task<bool> UpdateJob(CancellationToken ct, string via = null)
    {
        try
        {
            var response = await GetBlockTemplateAsync(ct);

            if(response.Error != null)
            {
                logger.Warn(() => $"Unable to update job. Daemon responded with: {response.Error.Message} Code {response.Error.Code}");
                return false;
            }

            var blockTemplate = response.Response;
            var job = currentJob;

            var isNew = job == null ||
                (blockTemplate != null &&
                    (job.BlockTemplate?.PreviousBlockhash != blockTemplate.PreviousBlockhash ||
                        blockTemplate.Height > job.BlockTemplate?.Height));

            if(isNew)
            {
                messageBus.NotifyChainHeight(poolConfig.Id, blockTemplate.Height, poolConfig.Template);

                if(via != null)
                    logger.Info(() => $"Detected new block {blockTemplate.Height} [{via}]");
                else
                    logger.Info(() => $"Detected new block {blockTemplate.Height}");

                // Update RandomX seed if needed
                UpdateSeedHash(blockTemplate);

                // Create new job
                job = new JunocashJob(blockTemplate, NextJobId(), randomXRealm);
                currentJob = job;

                // Update stats
                BlockchainStats.LastNetworkBlockTime = clock.Now;
                BlockchainStats.BlockHeight = blockTemplate.Height;
                BlockchainStats.NetworkDifficulty = job.NetworkDifficulty;
                BlockchainStats.NextNetworkTarget = blockTemplate.Target;
                BlockchainStats.NextNetworkBits = blockTemplate.Bits;
            }
            else
            {
                if(via != null)
                    logger.Debug(() => $"Template update {blockTemplate.Height} [{via}]");
                else
                    logger.Debug(() => $"Template update {blockTemplate.Height}");
            }

            return isNew;
        }
        catch(OperationCanceledException)
        {
            // ignored
        }
        catch(Exception ex)
        {
            logger.Error(ex, () => $"Error during {nameof(UpdateJob)}");
        }

        return false;
    }

    #endregion

    #region API Surface

    public void PrepareWorkerJob(JunocashWorkerJob workerJob, out string blob, out string target)
    {
        blob = null;
        target = null;

        var job = currentJob;
        if(job != null)
            job.PrepareWorkerJob(workerJob, out blob, out target);
    }

    public override JunocashJob GetJobForStratum()
    {
        return currentJob;
    }

    public async Task<bool> ValidateAddressAsync(string address, CancellationToken ct)
    {
        if(string.IsNullOrEmpty(address))
            return false;

        // Try t-addr validation first
        var result = await rpc.ExecuteAsync<ValidateAddressResponse>(logger,
            BitcoinCommands.ValidateAddress, ct, new[] { address });

        if(result.Response is { IsValid: true })
            return true;

        // Try z-addr validation
        var zResult = await rpc.ExecuteAsync<ValidateAddressResponse>(logger,
            EquihashCommands.ZValidateAddress, ct, new[] { address });

        return zResult.Response is { IsValid: true };
    }

    public async ValueTask<Share> SubmitShareAsync(StratumConnection worker,
        string minerNonce, string workerHash, JunocashWorkerJob workerJob, CancellationToken ct)
    {
        Contract.RequiresNonNull(worker);

        var context = worker.ContextAs<JunocashWorkerContext>();
        var job = currentJob;

        if(workerJob.Height != job?.Height)
            throw new StratumException(StratumError.MinusOne, "block expired");

        // Validate & process
        var (share, blockHex) = job.ProcessShare(minerNonce, workerJob.ExtraNonce1, workerHash, worker);

        // Enrich share with common data
        share.PoolId = poolConfig.Id;
        share.IpAddress = worker.RemoteEndpoint.Address.ToString();
        share.Miner = context.Miner;
        share.Worker = context.Worker;
        share.UserAgent = context.UserAgent;
        share.Source = clusterConfig.ClusterName;
        share.NetworkDifficulty = job.NetworkDifficulty;
        share.Created = clock.Now;

        // If block candidate, submit & check if accepted
        if(share.IsBlockCandidate)
        {
            logger.Info(() => $"Submitting block {share.BlockHeight} [{share.BlockHash[..6]}]");

            share.IsBlockCandidate = await SubmitBlockAsync(share, blockHex);

            if(share.IsBlockCandidate)
            {
                logger.Info(() => $"Daemon accepted block {share.BlockHeight} [{share.BlockHash[..6]}] submitted by {context.Miner}");

                OnBlockFound();

                // Store the coinbase tx hash for payment verification
                share.TransactionConfirmationData = share.BlockHash;
            }
            else
            {
                share.TransactionConfirmationData = null;
            }
        }

        return share;
    }

    #endregion

    #region Overrides

    protected override void ConfigureDaemons()
    {
        var jsonSerializerSettings = ctx.Resolve<JsonSerializerSettings>();
        rpc = new RpcClient(daemonEndpoints.First(), jsonSerializerSettings, messageBus, poolConfig.Id);
    }

    protected override async Task<bool> AreDaemonsHealthyAsync(CancellationToken ct)
    {
        var response = await rpc.ExecuteAsync<BlockchainInfo>(logger, BitcoinCommands.GetBlockchainInfo, ct);
        return response.Error == null;
    }

    protected override async Task<bool> AreDaemonsConnectedAsync(CancellationToken ct)
    {
        var response = await rpc.ExecuteAsync<PeerInfo[]>(logger, BitcoinCommands.GetPeerInfo, ct);
        return response.Error == null && response.Response != null && response.Response.Length > 0;
    }

    protected override async Task EnsureDaemonsSynchedAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

        var syncPendingNotificationShown = false;

        do
        {
            var response = await GetBlockTemplateAsync(ct);
            var isSynced = response.Error == null;

            if(isSynced)
            {
                logger.Info(() => "All daemons synced with blockchain");
                break;
            }

            if(!syncPendingNotificationShown)
            {
                logger.Info(() => "Daemon is still syncing with network. Manager will be started once synced.");
                syncPendingNotificationShown = true;
            }

            await ShowDaemonSyncProgressAsync(ct);
        } while(await timer.WaitForNextTickAsync(ct));
    }

    private async Task ShowDaemonSyncProgressAsync(CancellationToken ct)
    {
        var response = await rpc.ExecuteAsync<BlockchainInfo>(logger, BitcoinCommands.GetBlockchainInfo, ct);

        if(response.Response != null)
        {
            var info = response.Response;
            logger.Info(() => $"Daemon has downloaded {info.VerificationProgress * 100:0.00}% of blockchain");
        }
    }

    protected override async Task PostStartInitAsync(CancellationToken ct)
    {
        // Get blockchain info
        var infoResponse = await rpc.ExecuteAsync<BlockchainInfo>(logger, BitcoinCommands.GetBlockchainInfo, ct);

        if(infoResponse.Error != null)
            throw new PoolStartupException($"Init RPC failed: {infoResponse.Error.Message} (Code {infoResponse.Error.Code})", poolConfig.Id);

        var info = infoResponse.Response;

        // Detect network
        chainConfig = coin.GetNetwork(info.Chain.ToLower() switch
        {
            "main" => NBitcoin.ChainName.Mainnet,
            "test" => NBitcoin.ChainName.Testnet,
            "regtest" => NBitcoin.ChainName.Regtest,
            _ => throw new PoolStartupException($"Unsupported chain '{info.Chain}'", poolConfig.Id)
        });

        // Validate pool address
        var addressValid = await ValidateAddressAsync(poolConfig.Address, ct);
        if(!addressValid)
            throw new PoolStartupException($"Pool address '{poolConfig.Address}' is not valid", poolConfig.Id);

        // Update stats
        BlockchainStats.RewardType = "POW";
        BlockchainStats.NetworkType = info.Chain;

        await UpdateNetworkStatsAsync(ct);

        // Periodically update network stats
        Observable.Interval(TimeSpan.FromMinutes(1))
            .Select(_ => Observable.FromAsync(() =>
                Guard(() => UpdateNetworkStatsAsync(ct),
                    ex => logger.Error(ex))))
            .Concat()
            .Subscribe();

        // Make sure we have a current seed hash before starting jobs
        if(poolConfig.EnableInternalStratum == true)
        {
            using var seedTimer = new PeriodicTimer(TimeSpan.FromSeconds(5));

            do
            {
                var blockTemplate = await GetBlockTemplateAsync(ct);

                if(blockTemplate?.Response != null)
                {
                    UpdateSeedHash(blockTemplate.Response);
                    break;
                }

                logger.Info(() => "Waiting for first valid block template");
            } while(await seedTimer.WaitForNextTickAsync(ct));
        }

        SetupJobUpdates(ct);
    }

    private async Task UpdateNetworkStatsAsync(CancellationToken ct)
    {
        try
        {
            var infoResponse = await rpc.ExecuteAsync<BlockchainInfo>(logger, BitcoinCommands.GetBlockchainInfo, ct);

            if(infoResponse.Error != null)
            {
                logger.Warn(() => $"Error refreshing network stats: {infoResponse.Error.Message}");
                return;
            }

            if(infoResponse.Response != null)
            {
                BlockchainStats.ConnectedPeers = 0; // Will be updated below

                var peerResponse = await rpc.ExecuteAsync<PeerInfo[]>(logger, BitcoinCommands.GetPeerInfo, ct);
                if(peerResponse.Error == null && peerResponse.Response != null)
                    BlockchainStats.ConnectedPeers = peerResponse.Response.Length;

                // Network hashrate from difficulty: hashrate = difficulty * 2^13 / blocktime
                // For Junocash with ~60s block time: hashrate ≈ difficulty * 136.53
                var job = currentJob;
                if(job != null)
                    BlockchainStats.NetworkHashrate = job.NetworkDifficulty / 60; // rough estimate
            }
        }
        catch(Exception e)
        {
            logger.Error(e);
        }
    }

    protected virtual void SetupJobUpdates(CancellationToken ct)
    {
        var blockSubmission = blockFoundSubject.Synchronize();
        var pollTimerRestart = blockFoundSubject.Synchronize();

        var triggers = new List<IObservable<(string Via, string Data)>>
        {
            blockSubmission.Select(_ => (JobRefreshBy.BlockFound, (string) null))
        };

        // ZMQ block notifications
        var zmq = poolConfig.Daemons
            .Where(x => !string.IsNullOrEmpty(x.Extra.SafeExtensionDataAs<JunocashDaemonEndpointConfigExtra>()?.ZmqBlockNotifySocket))
            .ToDictionary(x => x, x =>
            {
                var extra = x.Extra.SafeExtensionDataAs<JunocashDaemonEndpointConfigExtra>();
                var topic = !string.IsNullOrEmpty(extra.ZmqBlockNotifyTopic?.Trim()) ? extra.ZmqBlockNotifyTopic.Trim() : BitcoinConstants.ZmqPublisherTopicBlockHash;
                return (Socket: extra.ZmqBlockNotifySocket, Topic: topic);
            });

        if(zmq.Count > 0)
        {
            logger.Info(() => $"Subscribing to ZMQ push-updates from {string.Join(", ", zmq.Values)}");

            var blockNotify = rpc.ZmqSubscribe(logger, ct, zmq)
                .Select(msg =>
                {
                    using(msg)
                    {
                        var result = msg[0].Read().ToHexString();
                        return result;
                    }
                })
                .DistinctUntilChanged()
                .Select(_ => (JobRefreshBy.PubSub, (string) null))
                .Publish()
                .RefCount();

            pollTimerRestart = Observable.Merge(
                    blockSubmission,
                    blockNotify.Select(_ => Unit.Default))
                .Publish()
                .RefCount();

            triggers.Add(blockNotify);
        }

        if(poolConfig.BlockRefreshInterval > 0)
        {
            var pollingInterval = poolConfig.BlockRefreshInterval > 0 ? poolConfig.BlockRefreshInterval : 1000;

            triggers.Add(Observable.Timer(TimeSpan.FromMilliseconds(pollingInterval))
                .TakeUntil(pollTimerRestart)
                .Select(_ => (JobRefreshBy.Poll, (string) null))
                .Repeat());
        }
        else
        {
            triggers.Add(Observable.Interval(TimeSpan.FromMilliseconds(1000))
                .Select(_ => (JobRefreshBy.Initial, (string) null))
                .TakeWhile(_ => !hasInitialBlockTemplate));
        }

        Blocks = triggers.Merge()
            .Select(x => Observable.FromAsync(() => UpdateJob(ct, x.Via)))
            .Concat()
            .Where(isNew => isNew)
            .Do(_ => hasInitialBlockTemplate = true)
            .Select(_ => Unit.Default)
            .Publish()
            .RefCount();
    }

    #endregion
}

public class JunocashDaemonEndpointConfigExtra
{
    public string ZmqBlockNotifySocket { get; set; }
    public string ZmqBlockNotifyTopic { get; set; }
}
