using System.Globalization;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Autofac;
using AutoMapper;
using Microsoft.IO;
using Miningcore.Blockchain.Cryptonote;
using Miningcore.Blockchain.Cryptonote.StratumRequests;
using Miningcore.Blockchain.Cryptonote.StratumResponses;
using Miningcore.Configuration;
using Miningcore.JsonRpc;
using Miningcore.Messaging;
using Miningcore.Mining;
using Miningcore.Nicehash;
using Miningcore.Notifications.Messages;
using Miningcore.Payments;
using Miningcore.Persistence;
using Miningcore.Persistence.Repositories;
using Miningcore.Stratum;
using Miningcore.Time;
using Newtonsoft.Json;
using static Miningcore.Util.ActionUtils;

namespace Miningcore.Blockchain.Junocash;

[CoinFamily(CoinFamily.Junocash)]
public class JunocashPool : PoolBase
{
    public JunocashPool(IComponentContext ctx,
        JsonSerializerSettings serializerSettings,
        IConnectionFactory cf,
        IStatsRepository statsRepo,
        IMapper mapper,
        IMasterClock clock,
        IMessageBus messageBus,
        RecyclableMemoryStreamManager rmsm,
        NicehashService nicehashService) :
        base(ctx, serializerSettings, cf, statsRepo, mapper, clock, messageBus, rmsm, nicehashService)
    {
    }

    private long currentJobId;
    private JunocashJobManager manager;
    private long extraNonceCounter;

    private string AssignExtraNonce()
    {
        var value = Interlocked.Increment(ref extraNonceCounter);
        return value.ToString("x16"); // 8-byte hex string
    }

    private async Task OnLoginAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<JunocashWorkerContext>();

        if(request.Id == null)
            throw new StratumException(StratumError.MinusOne, "missing request id");

        var loginRequest = request.ParamsAs<CryptonoteLoginRequest>();

        if(string.IsNullOrEmpty(loginRequest?.Login))
            throw new StratumException(StratumError.MinusOne, "missing login");

        // extract worker/miner
        var split = loginRequest.Login.Split('.');
        context.Miner = split[0].Trim();
        context.Worker = split.Length > 1 ? split[1].Trim() : null;
        context.UserAgent = loginRequest.UserAgent?.Trim();

        // validate address
        var result = await manager.ValidateAddressAsync(context.Miner, CancellationToken.None);

        context.IsSubscribed = result;
        context.IsAuthorized = result;

        if(context.IsAuthorized)
        {
            // Assign extraNonce1
            context.ExtraNonce1 = AssignExtraNonce();

            // extract control vars from password
            var passParts = loginRequest.Password?.Split(PasswordControlVarsSeparator);
            var staticDiff = GetStaticDiffFromPassparts(passParts);

            // Nicehash support
            var nicehashDiff = await GetNicehashStaticMinDiff(context, manager.Coin.Name, manager.Coin.GetAlgorithmName());

            if(nicehashDiff.HasValue)
            {
                if(!staticDiff.HasValue || nicehashDiff > staticDiff)
                {
                    logger.Info(() => $"[{connection.ConnectionId}] Nicehash detected. Using API supplied difficulty of {nicehashDiff.Value}");
                    staticDiff = nicehashDiff;
                }
                else
                    logger.Info(() => $"[{connection.ConnectionId}] Nicehash detected. Using miner supplied difficulty of {staticDiff.Value}");
            }

            // Static diff
            if(staticDiff.HasValue &&
               (context.VarDiff != null && staticDiff.Value >= context.VarDiff.Config.MinDiff ||
                   context.VarDiff == null && staticDiff.Value > context.Difficulty))
            {
                context.VarDiff = null;
                context.SetDifficulty(staticDiff.Value);
                logger.Info(() => $"[{connection.ConnectionId}] Static difficulty set to {staticDiff.Value}");
            }

            // respond
            var loginResponse = new CryptonoteLoginResponse
            {
                Id = connection.ConnectionId,
                Job = CreateWorkerJob(connection)
            };

            var response = new JsonRpcResponse<object>(loginResponse, request.Id);

            if(context.IsNicehash || poolConfig.EnableAsicBoost == true)
            {
                response.Extra = new Dictionary<string, object>();
                response.Extra["error"] = null;
            }

            await connection.RespondAsync(response);

            if(!string.IsNullOrEmpty(context.Worker))
                logger.Info(() => $"[{connection.ConnectionId}] Authorized worker {context.Worker}@{context.Miner}");
            else
                logger.Info(() => $"[{connection.ConnectionId}] Authorized miner {context.Miner}");
        }
        else
        {
            await connection.RespondErrorAsync(StratumError.MinusOne, "invalid login", request.Id);

            if(clusterConfig?.Banning?.BanOnLoginFailure is null or true)
            {
                logger.Info(() => $"[{connection.ConnectionId}] Banning unauthorized worker {context.Miner} for {loginFailureBanTimeout.TotalSeconds} sec");
                banManager.Ban(connection.RemoteEndpoint.Address, loginFailureBanTimeout);
                Disconnect(connection);
            }
        }
    }

    private async Task OnGetJobAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<JunocashWorkerContext>();

        if(request.Id == null)
            throw new StratumException(StratumError.MinusOne, "missing request id");

        var getJobRequest = request.ParamsAs<CryptonoteGetJobRequest>();

        if(connection.ConnectionId != getJobRequest?.WorkerId || !context.IsAuthorized)
            throw new StratumException(StratumError.MinusOne, "unauthorized");

        var job = CreateWorkerJob(connection);

        var response = new JsonRpcResponse<object>(job, request.Id);

        if(context.IsNicehash || poolConfig.EnableAsicBoost == true)
        {
            response.Extra = new Dictionary<string, object>();
            response.Extra["error"] = null;
        }

        await connection.RespondAsync(response);
    }

    private CryptonoteJobParams CreateWorkerJob(StratumConnection connection)
    {
        var context = connection.ContextAs<JunocashWorkerContext>();
        var job = new JunocashWorkerJob(NextJobId(), context.Difficulty);
        job.ExtraNonce1 = context.ExtraNonce1;

        manager.PrepareWorkerJob(job, out var blob, out var target);

        if(string.IsNullOrEmpty(blob) || string.IsNullOrEmpty(target))
            return null;

        var result = new CryptonoteJobParams
        {
            JobId = job.Id,
            Blob = blob,
            Target = target,
            Height = job.Height,
            SeedHash = job.SeedHash,
            Algorithm = "rx/juno",
        };

        lock(context)
        {
            context.AddJob(job, 4);
        }

        return result;
    }

    private async Task OnSubmitAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest, CancellationToken ct)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<JunocashWorkerContext>();

        try
        {
            if(request.Id == null)
                throw new StratumException(StratumError.MinusOne, "missing request id");

            if(!context.IsAuthorized)
                throw new StratumException(StratumError.MinusOne, "unauthorized");

            var requestAge = clock.Now - tsRequest.Timestamp.UtcDateTime;
            if(requestAge > maxShareAge)
            {
                logger.Warn(() => $"[{connection.ConnectionId}] Dropping stale share submission request (server overloaded?)");
                return;
            }

            var submitRequest = request.ParamsAs<CryptonoteSubmitShareRequest>();

            if(connection.ConnectionId != submitRequest?.WorkerId)
                throw new StratumException(StratumError.MinusOne, "cheater");

            context.LastActivity = clock.Now;

            JunocashWorkerJob job;

            lock(context)
            {
                var jobId = submitRequest?.JobId;
                if((job = context.GetJob(jobId)) == null)
                    throw new StratumException(StratumError.MinusOne, "invalid jobid");
            }

            // dupe check
            if(!job.Submissions.TryAdd(submitRequest.Nonce, true))
                throw new StratumException(StratumError.MinusOne, "duplicate share");

            var share = await manager.SubmitShareAsync(connection, submitRequest.Nonce, submitRequest.Hash, job, ct);

            var response = new JsonRpcResponse<object>(new CryptonoteResponseBase(), request.Id);

            if(context.IsNicehash || poolConfig.EnableAsicBoost == true)
            {
                response.Extra = new Dictionary<string, object>();
                response.Extra["error"] = null;
            }

            await connection.RespondAsync(response);

            messageBus.SendMessage(share);

            PublishTelemetry(TelemetryCategory.Share, clock.Now - tsRequest.Timestamp.UtcDateTime, true);

            logger.Info(() => $"[{connection.ConnectionId}] Share accepted: D={Math.Round(share.Difficulty, 3)}");

            if(share.IsBlockCandidate)
                poolStats.LastPoolBlockTime = clock.Now;

            context.Stats.ValidShares++;

            await UpdateVarDiffAsync(connection, false, ct);
        }
        catch(StratumException ex)
        {
            PublishTelemetry(TelemetryCategory.Share, clock.Now - tsRequest.Timestamp.UtcDateTime, false);

            context.Stats.InvalidShares++;
            logger.Info(() => $"[{connection.ConnectionId}] Share rejected: {ex.Message} [{context.UserAgent}]");

            ConsiderBan(connection, context, poolConfig.Banning);

            throw;
        }
    }

    private string NextJobId()
    {
        return Interlocked.Increment(ref currentJobId).ToString(CultureInfo.InvariantCulture);
    }

    private async Task OnNewJobAsync()
    {
        logger.Info(() => "Broadcasting jobs");

        await Guard(() => ForEachMinerAsync(async (connection, ct) =>
        {
            var job = CreateWorkerJob(connection);
            await connection.NotifyAsync(CryptonoteStratumMethods.JobNotify, job);
        }));
    }

    #region Overrides

    protected override async Task SetupJobManager(CancellationToken ct)
    {
        manager = ctx.Resolve<JunocashJobManager>();
        manager.Configure(poolConfig, clusterConfig);

        await manager.StartAsync(ct);

        if(poolConfig.EnableInternalStratum == true)
        {
            disposables.Add(manager.Blocks
                .Select(_ => Observable.FromAsync(() =>
                    Guard(OnNewJobAsync,
                        ex => logger.Debug(() => $"{nameof(OnNewJobAsync)}: {ex.Message}"))))
                .Concat()
                .Subscribe(_ => { }, ex =>
                {
                    logger.Debug(ex, nameof(OnNewJobAsync));
                }));

            await manager.Blocks.Take(1).ToTask(ct);
        }
        else
        {
            disposables.Add(manager.Blocks.Subscribe());
        }
    }

    protected override async Task InitStatsAsync(CancellationToken ct)
    {
        await base.InitStatsAsync(ct);

        blockchainStats = manager.BlockchainStats;
    }

    protected override WorkerContextBase CreateWorkerContext()
    {
        return new JunocashWorkerContext();
    }

    protected override async Task OnRequestAsync(StratumConnection connection,
        Timestamped<JsonRpcRequest> tsRequest, CancellationToken ct)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<JunocashWorkerContext>();

        try
        {
            switch(request.Method)
            {
                case CryptonoteStratumMethods.Login:
                    await OnLoginAsync(connection, tsRequest);
                    break;

                case CryptonoteStratumMethods.GetJob:
                    await OnGetJobAsync(connection, tsRequest);
                    break;

                case CryptonoteStratumMethods.Submit:
                    await OnSubmitAsync(connection, tsRequest, ct);
                    break;

                case CryptonoteStratumMethods.KeepAlive:
                    context.LastActivity = clock.Now;

                    var response = new JsonRpcResponse<object>(new CryptonoteKeepAliveResponse(), request.Id);

                    if(context.IsNicehash || poolConfig.EnableAsicBoost == true)
                    {
                        response.Extra = new Dictionary<string, object>();
                        response.Extra["error"] = null;
                    }

                    await connection.RespondAsync(response);
                    break;

                default:
                    logger.Debug(() => $"[{connection.ConnectionId}] Unsupported RPC request: {JsonConvert.SerializeObject(request, serializerSettings)}");
                    await connection.RespondErrorAsync(StratumError.Other, $"Unsupported request {request.Method}", request.Id);
                    break;
            }
        }
        catch(StratumException ex)
        {
            await connection.RespondErrorAsync(ex.Code, ex.Message, request.Id, false);
        }
    }

    public override double HashrateFromShares(double shares, double interval)
    {
        var result = shares / interval;
        return result;
    }

    public override double ShareMultiplier => 1;

    protected override async Task OnVarDiffUpdateAsync(StratumConnection connection, double newDiff, CancellationToken ct)
    {
        await base.OnVarDiffUpdateAsync(connection, newDiff, ct);

        if(connection.Context.ApplyPendingDifficulty())
        {
            var job = CreateWorkerJob(connection);
            await connection.NotifyAsync(CryptonoteStratumMethods.JobNotify, job);
        }
    }

    #endregion
}
