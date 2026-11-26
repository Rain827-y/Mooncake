// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
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

}

struct MemoryEngine::Impl {
    ~Impl();
    Status Start(const Config &config);
    Status StartCallbacks(std::string host);
    Status Register(const std::optional<uint32_t> &requestedRank, Milliseconds heartbeat);
    Status CreateTransferEngine(const Config &config);
    void StartHeartbeat(Milliseconds interval);

    Layout mLayout;
    uint32_t mRank{};
    std::string mServer;
    std::string mId;
    std::string mCallbackAddress;
    bool mRegistrationAttempted{};
    MemoryEngineRpc mRpc;
    std::unique_ptr<TransferEngine> mTent;
    std::jthread mHeartbeat;
};

MemoryEngine::Impl::~Impl()
{
    mHeartbeat = std::jthread();
    mTent.reset();
    if (mRegistrationAttempted) {
        const auto request = EncodeCancelRequest({mId, mCallbackAddress});
        const auto status = Call(mRpc, mServer, MemoryRpc::Cancel, request);
        if (!status.ok()) [[unlikely]] {
            LOG(ERROR) << "MemoryEngine Cancel RPC failed, registration ID:" << mId
                       << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        }
    }
    const auto stopped = mRpc.Stop();
    if (!stopped.ok()) [[unlikely]] {
        LOG(ERROR) << "Failed to stop MemoryEngine callback RPC, registration ID:" << mId
                   << ", status:" << static_cast<uint16_t>(stopped.code()) << ".";
    }
}

Status MemoryEngine::Impl::StartCallbacks(std::string host)
{
    bool ipv6 = false;
    const auto status = host.empty() ? discoverLocalIpAddress(host, ipv6) : checkLocalIpAddress(host, ipv6);
    CHECK_STATUS(status);
    uint16_t port{};
    CHECK_STATUS(mRpc.Start(port, ipv6));
    mCallbackAddress = buildIpAddrWithPort(host, port, ipv6);
    return Status::OK();
}

Status MemoryEngine::Impl::Register(const std::optional<uint32_t> &requestedRank, Milliseconds heartbeat)
{
    mId = NewRegistrationId();
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
    mTent = std::move(tent);
    return Status::OK();
#endif
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

Status MemoryEngine::Impl::Start(const Config &config)
{
    ClientOptions options;
    CHECK_STATUS(ReadClientOptions(config, options));
    mLayout = options.layout;
    const auto &server = options.server;
    mServer = buildIpAddrWithPort(server.host, server.port, server.ipv6);
    CHECK_STATUS(StartCallbacks(std::move(options.callbackHost)));
    CHECK_STATUS(Register(options.requestedRank, options.heartbeat));
    CHECK_STATUS(CreateTransferEngine(config));
    const auto status = Call(mRpc, mServer, MemoryRpc::Confirm, EncodeConfirmRequest({mId, mRank}));
    if (!status.ok()) [[unlikely]] {
        LOG(ERROR) << "MemoryEngine Confirm request failed, registration ID:" << mId << ", rank:" << mRank
                   << ", status:" << static_cast<uint16_t>(status.code()) << ".";
        return status;
    }
    StartHeartbeat(options.heartbeat);
    return Status::OK();
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
    const auto status = result->mImpl->Start(*config.freeze());
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

}
}
