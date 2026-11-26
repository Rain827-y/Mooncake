// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include "tent/common/utils/ip.h"
#include "tent/transfer_engine.h"
#include "memory_engine_common.h"
#include "memory_engine_client.h"

namespace mooncake {
namespace tent {
namespace {

constexpr auto DEFAULT_HEARTBEAT = Milliseconds(3000);
constexpr int REGISTRATION_ID_WORDS = 4;
#ifdef USE_MEMFABRIC
constexpr char TENT_METADATA_TYPE[] = "metadata_type";
constexpr char TENT_METADATA_SERVERS[] = "metadata_servers";
constexpr char TENT_MEMFABRIC_ENABLED[] = "transports/memfabric/enable";
constexpr char TENT_GVA_CONFIG[] = "transports/memfabric/gva";
#endif

struct ClientOptions {
    Endpoint server;
    std::string callbackHost;
    Layout layout;
    std::optional<uint32_t> requestedRank;
    Milliseconds heartbeat{DEFAULT_HEARTBEAT};
};

struct RemoteRegion {
    size_t size{};
    bool mapped{};
    bool revoked{};
};

struct PeerMapping {
    SegmentID segment{LOCAL_SEGMENT_ID};
    uint64_t generation{};
    bool revoked{};
    std::string segmentName;
    std::map<uint64_t, RemoteRegion> regions;
};

Status ReadClientOptions(const Config &config, ClientOptions &options)
{
    if (!config.get(config_keys::CONFIG_ENABLED, true)) {
        return Status::NotImplemented("MemoryEngine is disabled");
    }
    CHECK_STATUS(ParseEndpoint(config.get(config_keys::CONFIG_SERVER_URL, ""), options.server));
    CHECK_STATUS(ReadLayout(config, config_keys::CONFIG_WORLD_SIZE, config_keys::CONFIG_HOST_MEM_MAX,
                            config_keys::CONFIG_DEVICE_MEM_MAX, options.layout));
    CHECK_STATUS(ValidateLayout(options.layout));
    if (!config.get(config_keys::CONFIG_AUTO_RANKING, true)) {
        const auto rank =
            ReadUnsigned(config.get(config_keys::CONFIG_LOCAL_RANK, json()), options.layout.worldSize - 1);
        if (!rank) {
            return Status::InvalidArgument("Explicit local_rank is required and must be in range");
        }
        options.requestedRank = static_cast<uint32_t>(*rank);
    }
    options.callbackHost = config.get(config_keys::CONFIG_CLIENT_HOST, "");
    options.heartbeat = Milliseconds(config.get(config_keys::CONFIG_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT.count()));
    if (options.heartbeat <= Milliseconds::zero() || options.heartbeat >= MAX_TIMEOUT) {
        return Status::InvalidArgument("Invalid MemoryEngine heartbeat interval");
    }
    return Status::OK();
}

std::string NewRegistrationId()
{
    std::random_device random;
    std::ostringstream id;
    id << std::hex;
    for (int index = 0; index < REGISTRATION_ID_WORDS; ++index) {
        id << random() << '-';
    }
    return id.str();
}

Status CallControl(MemoryEngineRpc &rpc, MemoryRpc op, const PeerCall &server, Clock::time_point deadline)
{
    do {
        auto status = CallAsync(rpc, op, server, deadline).get();
        if (!Retryable(status)) {
            return status;
        }
        std::this_thread::sleep_until(std::min(deadline, Clock::now() + RETRY_INTERVAL));
    } while (Clock::now() < deadline);
    return Status::TimedOut("MemoryEngine control request timed out");
}

}

struct MemoryEngine::Impl {
    ~Impl();
    Status Start(std::shared_ptr<const Config> config);
    Status StartCallbacks(std::string host);
    Status Register(const std::optional<uint32_t> &requestedRank, Milliseconds heartbeat);
    Status CreateTransferEngine(const Config &config);
    Status InitializeRegistration();
    Status ConfirmRegistration();
    void StartHeartbeat(Milliseconds interval);
    Status Join();
    Status Leave();
    Status SharePeer(const json &payload);
    Status SharePeers(const json &payload);
    Status UnsharePeer(const json &payload);
    Status OpenSharedPeer(const std::string &id, const SharedPeer &request);
    Status ValidatePeerRequest(const std::string &id, uint32_t rank) const;
    Status MapPeerRegion(uint32_t rank, PeerMapping &peer, const GvaRegion &region);
    Status DrainPeerMappings(const PeerMapping &peer);
    Status ClosePeer(PeerMapping &peer);
    Status ClearPeers();
    Status ReleaseMembership();
    Status CancelRegistration();

    std::mutex mApiMutex;
    std::mutex mPeerMutex;
    std::shared_ptr<const Config> mConfig;
    Layout mLayout;
    std::optional<uint32_t> mRequestedRank;
    uint32_t mRank{};
    std::string mServer;
    std::string mId;
    std::string mCallbackAddress;
    bool mRegistrationAttempted{};
    bool mJoinAttempted{};
    bool mJoined{};
    bool mLeaving{};
    Milliseconds mJoinTimeout{DEFAULT_REGISTRATION};
    Milliseconds mHeartbeatInterval{DEFAULT_HEARTBEAT};
    std::unordered_map<uint32_t, PeerMapping> mPeers;
    MemoryEngineRpc mRpc;
    std::unique_ptr<TransferEngine> mTent;
    std::jthread mHeartbeat;
};

MemoryEngine::Impl::~Impl()
{
    if (mJoinAttempted || mLeaving) {
        const auto status = ReleaseMembership();
        if (!status.ok()) [[unlikely]] {
            LOG(ERROR) << "MemoryEngine destructor could not release membership, registration ID:" << mId
                       << ", rank:" << mRank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        }
    }
    mHeartbeat = std::jthread();
    const auto stopped = mRpc.Stop();
    if (!stopped.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to stop MemoryEngine callback RPC, registration ID:" << mId
                   << ", status:" << static_cast<uint16_t>(stopped.code()) << ".";
    }
    mTent.reset();
    if (!mJoinAttempted && !mLeaving) {
        const auto status = CancelRegistration();
        if (!status.ok()) [[unlikely]] {
            LOG(ERROR) << "MemoryEngine Cancel RPC failed, registration ID:" << mId
                       << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        }
    }
}

Status MemoryEngine::Impl::StartCallbacks(std::string host)
{
    bool ipv6 = false;
    const auto status = host.empty() ? discoverLocalIpAddress(host, ipv6) : checkLocalIpAddress(host, ipv6);
    CHECK_STATUS(status);
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::SharePeer,
                                 [this](const json &request, json &) { return SharePeer(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::SharePeers,
                                 [this](const json &request, json &) { return SharePeers(request); }, true));
    CHECK_STATUS(RegisterHandler(mRpc, MemoryRpc::UnsharePeer,
                                 [this](const json &request, json &) { return UnsharePeer(request); }, true));
    uint16_t port{};
    CHECK_STATUS(mRpc.Start(port, ipv6));
    mCallbackAddress = buildIpAddrWithPort(host, port, ipv6);
    return Status::OK();
}

Status MemoryEngine::Impl::Register(const std::optional<uint32_t> &requestedRank, Milliseconds heartbeat)
{
    {
        std::lock_guard lock(mPeerMutex);
        mId = NewRegistrationId();
        mPeers.clear();
    }
    const RegisterRequest request{mId, mLayout, mCallbackAddress, requestedRank};
    mRegistrationAttempted = true;
    json reply;
    const auto status = Call(mRpc, mServer, MemoryRpc::Register, EncodeRegisterRequest(request), reply);
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine Register RPC failed, registration ID:" << mId << ", requested rank:"
                   << (requestedRank ? std::to_string(*requestedRank) : "auto")
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    RegisterResponse response;
    const auto decodeRet = DecodeRegisterResponse(reply, mLayout.worldSize, response);
    if (!decodeRet.ok()) [[unlikely]] {
        LOG(ERROR) << "Invalid MemoryEngine Register response, registration ID:" << mId
                   << ", error:" << decodeRet.message() << ".";
        return decodeRet;
    }
    if (static_cast<uint64_t>(heartbeat.count()) >= response.leaseMs) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine heartbeat interval exceeds or equals rank lease, registration ID:" << mId
                   << ", rank:" << response.rank << ", heartbeat interval ms:" << heartbeat.count()
                   << ", rank lease ms:" << response.leaseMs << ".";
        return Status::InvalidArgument("MemoryEngine heartbeat interval must be shorter than the server lease");
    }
    std::lock_guard lock(mPeerMutex);
    mRank = response.rank;
    return Status::OK();
}

Status MemoryEngine::Impl::CreateTransferEngine(const Config &config)
{
#ifndef USE_MEMFABRIC
    static_cast<void>(config);
    LOG(ERROR) << "MemoryEngine requires MemFabric support, registration ID:" << mId << ", rank:" << mRank << ".";
    return Status::NotImplemented("MemoryEngine requires TENT with MemFabric support");
#else
    auto native = std::make_shared<Config>();
    CHECK_STATUS(native->load(config.dump()));
    native->set(TENT_METADATA_TYPE, "p2p");
    native->set(TENT_METADATA_SERVERS, "");
    native->set(TENT_MEMFABRIC_ENABLED, true);
    const json gva{{"enabled", true}, {"rank", mRank}, {"rank_count", mLayout.worldSize},
                   {"dram_window", mLayout.hostWindow / GIB}, {"hbm_window", mLayout.deviceWindow / GIB}};
    for (const auto &[key, value] : gva.items()) {
        native->set(std::string(TENT_GVA_CONFIG) + '/' + key, value);
    }

    auto tent = std::unique_ptr<TransferEngine>(new (std::nothrow) TransferEngine(native));
    if (!tent || !tent->available()) [[unlikely]] {
        LOG(ERROR) << "Failed to initialize MemoryEngine TENT instance, registration ID:" << mId
                   << ", rank:" << mRank << ".";
        return Status::InternalError("Could not initialize the MemoryEngine TENT instance");
    }
    // TENT can reload MC_TENT_CONF in its constructor. Check the effective layout.
    const auto effective = native->get(TENT_GVA_CONFIG, json::object());
    for (const auto &[key, value] : gva.items()) {
        const auto entry = effective.find(key);
        if (entry == effective.end() || *entry != value) [[unlikely]] {
            LOG(ERROR) << "MemoryEngine TENT GVA configuration changed, registration ID:" << mId
                       << ", rank:" << mRank << ", key:" << key << ".";
            return Status::InvalidArgument("TENT configuration changed the registered GVA layout");
        }
    }
    if (!native->get(TENT_MEMFABRIC_ENABLED, false)) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine TENT MemFabric is disabled, registration ID:" << mId << ", rank:" << mRank << ".";
        return Status::InvalidArgument("TENT configuration disabled MemFabric");
    }
    std::lock_guard lock(mPeerMutex);
    mTent = std::move(tent);
    return Status::OK();
#endif
}

Status MemoryEngine::Impl::ValidatePeerRequest(const std::string &id, uint32_t rank) const
{
    if (id != mId || rank >= mLayout.worldSize || rank == mRank) [[unlikely]] {
        LOG(ERROR) << "Invalid MemoryEngine peer callback, request ID:" << id << ", registration ID:" << mId
                   << ", rank:" << mRank << ", peer rank:" << rank << ".";
        return Status::InvalidArgument("Invalid MemoryEngine peer callback");
    }
    return Status::OK();
}

Status MemoryEngine::Impl::DrainPeerMappings(const PeerMapping &peer)
{
    Status result;
    for (const auto &[address, region] : peer.regions) {
        const auto status = mTent->unmapRemoteMemory(peer.segment, reinterpret_cast<void *>(address), MEMFABRIC);
        // A successful unmap means the MemFabric rank has been cleaned.
        if (status.ok()) {
            return status;
        }
        // An attempt that failed before native import has no mapping to clear.
        if (!status.IsInvalidArgument() || region.mapped) {
            result = status;
        }
    }
    return result;
}

Status MemoryEngine::Impl::ClosePeer(PeerMapping &peer)
{
    if (peer.segment != LOCAL_SEGMENT_ID) {
        CHECK_STATUS(DrainPeerMappings(peer));
        CHECK_STATUS(mTent->closeSegment(peer.segment));
        peer.segment = LOCAL_SEGMENT_ID;
    }
    peer.regions.clear();
    peer.segmentName.clear();
    return Status::OK();
}

Status MemoryEngine::Impl::OpenSharedPeer(const std::string &id, const SharedPeer &request)
{
    CHECK_STATUS(ValidatePeerRequest(id, request.rank));
    if (!mTent || request.segment == mTent->getSegmentName()) [[unlikely]] {
        return Status::InvalidEntry("Invalid MemoryEngine peer segment");
    }
    auto &peer = mPeers[request.rank];
    if (peer.generation > request.generation || (peer.generation == request.generation && peer.revoked)) [[unlikely]] {
        return Status::InvalidEntry("MemoryEngine peer generation was replaced or revoked");
    }
    if (peer.generation < request.generation) {
        CHECK_STATUS(ClosePeer(peer));
        peer.generation = request.generation;
        peer.revoked = false;
    }
    if (peer.segment != LOCAL_SEGMENT_ID) {
        return peer.segmentName == request.segment ? Status::OK() :
            Status::InvalidEntry("MemoryEngine peer changed its segment");
    }
    CHECK_STATUS(mTent->openSegment(peer.segment, request.segment));
    peer.segmentName = request.segment;
    return Status::OK();
}

Status MemoryEngine::Impl::MapPeerRegion(uint32_t rank, PeerMapping &peer, const GvaRegion &region)
{
    auto &mapped = peer.regions.try_emplace(region.address, RemoteRegion{region.size}).first->second;
    if (mapped.revoked || mapped.size != region.size) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine peer region conflicts with existing record, registration ID:" << mId
                   << ", rank:" << mRank << ", peer rank:" << rank << ", bytes:" << region.size << ".";
        return Status::InvalidEntry("MemoryEngine peer region conflicts with existing record");
    }
    if (mapped.mapped) {
        return Status::OK();
    }
    const auto status = mTent->mapRemoteMemory(peer.segment, reinterpret_cast<void *>(region.address), region.size);
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to map MemoryEngine peer region, registration ID:" << mId << ", rank:" << mRank
                   << ", peer rank:" << rank << ", bytes:" << region.size
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    mapped.mapped = true;
    return Status::OK();
}

Status MemoryEngine::Impl::SharePeer(const json &payload)
{
    PeerRequest header;
    CHECK_STATUS(DecodePeerRequest(payload, header));
    SharedPeer request{header.rank, header.generation, {}, {}};
    CHECK_STATUS(DecodeSharePeerDetails(payload, request));
    std::lock_guard lock(mPeerMutex);
    return OpenSharedPeer(header.id, request);
}

Status MemoryEngine::Impl::SharePeers(const json &payload)
{
    SharePeersRequest request;
    CHECK_STATUS(DecodeSharePeersRequest(payload, mLayout.worldSize, request));
    std::lock_guard lock(mPeerMutex);
    for (const auto &shared : request.peers) {
        CHECK_STATUS(OpenSharedPeer(request.id, shared));
    }
    for (const auto &shared : request.peers) {
        auto &peer = mPeers.at(shared.rank);
        for (const auto &region : shared.regions) {
            CHECK_STATUS(MapPeerRegion(shared.rank, peer, region));
        }
    }
    return Status::OK();
}

Status MemoryEngine::Impl::UnsharePeer(const json &payload)
{
    PeerRequest request;
    CHECK_STATUS(DecodePeerRequest(payload, request));
    std::lock_guard lock(mPeerMutex);
    CHECK_STATUS(ValidatePeerRequest(request.id, request.rank));
    auto &peer = mPeers[request.rank];
    if (peer.generation > request.generation) {
        return Status::OK();
    }
    peer.revoked = true;
    peer.generation = request.generation;
    return ClosePeer(peer);
}

Status MemoryEngine::Impl::Join()
{
    std::lock_guard lock(mApiMutex);
    if (mLeaving) {
        return Status::TooManyRequests("Retry MemoryEngine leave before rejoining");
    }
    if (mJoined) {
        return Status::OK();
    }
    if (!mTent) {
        CHECK_STATUS(InitializeRegistration());
    }
    const PeerCall server{mServer, EncodeJoinRequest({mId, mTent->getSegmentName()})};
    mJoinAttempted = true;
    const auto status = CallControl(mRpc, MemoryRpc::Join, server, Clock::now() + mJoinTimeout + RPC_TIMEOUT);
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine Join request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    mJoined = true;
    return Status::OK();
}

Status MemoryEngine::Impl::Leave()
{
    std::lock_guard lock(mApiMutex);
    if (!mRegistrationAttempted) {
        return Status::OK();
    }
    return ReleaseMembership();
}

Status MemoryEngine::Impl::ClearPeers()
{
    std::lock_guard lock(mPeerMutex);
    for (auto &[rank, peer] : mPeers) {
        const auto status = ClosePeer(peer);
        if (!status.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to close MemoryEngine peer, registration ID:" << mId << ", rank:" << mRank
                       << ", peer rank:" << rank << ", status:" << static_cast<uint16_t>(status.code()) << ".";
            return status;
        }
    }
    mPeers.clear();
    mTent.reset();
    return Status::OK();
}

Status MemoryEngine::Impl::ReleaseMembership()
{
    mLeaving = true;
    mJoined = false;
    const PeerCall server{mServer, EncodeRegistrationIdRequest({mId})};
    const auto begin = CallControl(mRpc, MemoryRpc::BeginLeave, server, Clock::now() + mJoinTimeout);
    if (!begin.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine BeginLeave failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(begin.code()) << ".";
        return begin;
    }
    CHECK_STATUS(ClearPeers());
    const auto finish = CallAsync(mRpc, MemoryRpc::FinishLeave, server, Clock::now() + RPC_TIMEOUT).get();
    if (!finish.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine FinishLeave failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(finish.code()) << ".";
        return finish;
    }
    mHeartbeat = std::jthread();
    mJoinAttempted = false;
    mRegistrationAttempted = false;
    mLeaving = false;
    return Status::OK();
}

Status MemoryEngine::Impl::CancelRegistration()
{
    if (!mRegistrationAttempted) {
        return Status::OK();
    }
    const auto status = Call(mRpc, mServer, MemoryRpc::Cancel, EncodeCancelRequest({mId, mCallbackAddress}));
    if (status.ok()) {
        mRegistrationAttempted = false;
    }
    return status;
}

void MemoryEngine::Impl::StartHeartbeat(Milliseconds interval)
{
    mHeartbeat = std::jthread([this, interval](std::stop_token stop) {
        auto lastCode = Status::Code::kOk;
        const auto request = EncodeRegistrationIdRequest({mId});
        RunPeriodic(stop, interval, [&] {
            const auto status = Call(mRpc, mServer, MemoryRpc::Heartbeat, request);
            const auto code = status.code();
            if (!status.ok() && code != lastCode) {
                if (status.IsInvalidEntry()) {
                    LOG(ERROR) << "MemoryEngine server rejected the heartbeat registration, registration ID:" << mId
                               << ", rank:" << mRank << ", status:" << static_cast<uint16_t>(code) << ".";
                } else {
                    LOG(WARNING) << "MemoryEngine Heartbeat RPC failed, registration ID:" << mId << ", rank:" << mRank
                                 << ", status:" << static_cast<uint16_t>(code) << ".";
                }
            }
            lastCode = code;
        });
    });
}

Status MemoryEngine::Impl::Start(std::shared_ptr<const Config> config)
{
    ClientOptions options;
    CHECK_STATUS(ReadClientOptions(*config, options));
    mJoinTimeout = Milliseconds(config->get(config_keys::CONFIG_JOIN_TIMEOUT_MS, DEFAULT_REGISTRATION.count()));
    if (mJoinTimeout <= Milliseconds::zero() || mJoinTimeout >= MAX_TIMEOUT) {
        LOG(ERROR) << "Invalid MemoryEngine join timeout, milliseconds:" << mJoinTimeout.count() << ".";
        return Status::InvalidArgument("Invalid MemoryEngine join timeout");
    }
    mConfig = std::move(config);
    mLayout = options.layout;
    mRequestedRank = options.requestedRank;
    mHeartbeatInterval = options.heartbeat;
    const auto &server = options.server;
    mServer = buildIpAddrWithPort(server.host, server.port, server.ipv6);
    CHECK_STATUS(StartCallbacks(std::move(options.callbackHost)));
    return InitializeRegistration();
}

Status MemoryEngine::Impl::InitializeRegistration()
{
    CHECK_STATUS(CancelRegistration());
    const auto status = ConfirmRegistration();
    if (!status.ok()) [[unlikely]] {
        auto cleanup = ClearPeers();
        if (cleanup.ok()) {
            cleanup = CancelRegistration();
        }
        if (!cleanup.ok()) [[unlikely]] {
            LOG(ERROR) << "Failed to roll back MemoryEngine registration, registration ID:" << mId
                       << ", rank:" << mRank << ", status:" << static_cast<uint16_t>(cleanup.code()) << ".";
        }
        return status;
    }
    StartHeartbeat(mHeartbeatInterval);
    return Status::OK();
}

Status MemoryEngine::Impl::ConfirmRegistration()
{
    CHECK_STATUS(Register(mRequestedRank, mHeartbeatInterval));
    CHECK_STATUS(CreateTransferEngine(*mConfig));
    const auto status = Call(mRpc, mServer, MemoryRpc::Confirm, EncodeConfirmRequest({mId, mRank}));
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine Confirm request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
    }
    return status;
}

MemoryEngine::MemoryEngine(std::unique_ptr<Impl> impl) : mImpl(std::move(impl))
{
}

MemoryEngine::~MemoryEngine() = default;

Status MemoryEngine::Create(const Config &config, std::unique_ptr<MemoryEngine> *engine)
{
    if (!engine || *engine) {
        return Status::InvalidArgument("MemoryEngine requires an empty output");
    }
    auto impl = std::unique_ptr<Impl>(new (std::nothrow) Impl);
    if (!impl) {
        return Status::InternalError("Could not allocate MemoryEngine state");
    }
    auto result = std::unique_ptr<MemoryEngine>(new (std::nothrow) MemoryEngine(std::move(impl)));
    if (!result) {
        return Status::InternalError("Could not allocate MemoryEngine");
    }
    const auto status = result->mImpl->Start(config.freeze());
    if (!status.ok()) {
        LOG(ERROR) << "Failed to create MemoryEngine, status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    *engine = std::move(result);
    return Status::OK();
}

Status MemoryEngine::Create(std::unique_ptr<MemoryEngine> *engine)
{
    Config config;
    CHECK_STATUS(LoadMemoryEngineConfig(config));
    return Create(config, engine);
}

Status MemoryEngine::Join()
{
    return mImpl->Join();
}

Status MemoryEngine::Leave()
{
    return mImpl->Leave();
}

}
}
