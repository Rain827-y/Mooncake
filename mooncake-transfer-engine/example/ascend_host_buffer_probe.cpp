// Copyright 2026 Huawei Technologies Co., Ltd
// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include "acl/acl.h"
#include "transfer_engine.h"

DEFINE_string(metadata_server, "P2PHANDSHAKE",
              "Metadata connection string used by TransferEngine");
DEFINE_string(local_ip, "127.0.0.1", "Local IP used by TransferEngine");
DEFINE_string(probe_dir, ".", "Directory used to create the temporary O_DIRECT probe file");
DEFINE_uint64(buffer_size, 4ULL * 1024ULL * 1024ULL, "Probe buffer size in bytes");
DEFINE_uint64(rpc_port, 17991, "Local RPC port used by TransferEngine");
DEFINE_int32(device_logic_id, 0, "Ascend device logic ID");
DEFINE_int32(device_phy_id, 0, "Ascend device physical ID");

namespace {
constexpr size_t DIRECT_IO_ALIGNMENT = 4096;
constexpr uint8_t PATTERN_SEED = 0x5A;

struct ProbeResources {
    void *hostBuffer = nullptr;
    void *deviceBuffer = nullptr;
    aclrtContext context = nullptr;
    std::string filePath;
    bool hostRegistered = false;
};

void FillPattern(void *buffer, size_t size)
{
    auto *bytes = static_cast<uint8_t *>(buffer);
    for (size_t i = 0; i < size; ++i) {
        bytes[i] = static_cast<uint8_t>(PATTERN_SEED + i);
    }
}

bool WriteAll(int fd, const void *buffer, size_t size)
{
    const auto *bytes = static_cast<const uint8_t *>(buffer);
    size_t written = 0;
    while (written < size) {
        ssize_t ret = write(fd, bytes + written, size - written);
        if (ret <= 0) {
            return false;
        }
        written += static_cast<size_t>(ret);
    }
    return true;
}

bool CreateProbeFile(ProbeResources &resources, size_t size)
{
    std::string pathTemplate = FLAGS_probe_dir + "/mooncake-direct-io-probe-XXXXXX";
    char *path = pathTemplate.data();
    int fd = mkstemp(path);
    if (fd < 0) {
        LOG(ERROR) << "Create probe file failed, errno:" << errno << ".";
        return false;
    }
    resources.filePath = path;
    bool success = WriteAll(fd, resources.hostBuffer, size) && fsync(fd) == 0;
    if (close(fd) != 0) {
        success = false;
    }
    if (!success) {
        LOG(ERROR) << "Initialize probe file failed, errno:" << errno << ".";
    }
    return success;
}

bool ProbeDirectRead(const ProbeResources &resources, size_t size)
{
    int fd = open(resources.filePath.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
    if (fd < 0) {
        LOG(ERROR) << "Open probe file with O_DIRECT failed, errno:" << errno << ".";
        return false;
    }
    ssize_t ret = pread(fd, resources.hostBuffer, size, 0);
    int savedErrno = errno;
    bool closeSuccess = close(fd) == 0;
    if (ret != static_cast<ssize_t>(size)) {
        LOG(ERROR) << "O_DIRECT pread failed, result:" << ret << ", expected:" << size
                   << ", errno:" << savedErrno << ".";
        return false;
    }
    if (!closeSuccess) {
        LOG(ERROR) << "Close O_DIRECT probe file failed, errno:" << errno << ".";
        return false;
    }
    return true;
}

bool VerifyPattern(const void *buffer, size_t size)
{
    const auto *bytes = static_cast<const uint8_t *>(buffer);
    for (size_t i = 0; i < size; ++i) {
        if (bytes[i] != static_cast<uint8_t>(PATTERN_SEED + i)) {
            LOG(ERROR) << "Verify probe pattern failed, offset:" << i << ".";
            return false;
        }
    }
    return true;
}

bool ProbeDeviceCopy(ProbeResources &resources, size_t size)
{
    aclError ret = aclrtMalloc(&resources.deviceBuffer, size, ACL_MEM_MALLOC_NORMAL_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Allocate device buffer failed, ret:" << ret << ".";
        return false;
    }
    ret = aclrtMemcpy(resources.deviceBuffer, size, resources.hostBuffer, size, ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Copy host buffer to device failed, ret:" << ret << ".";
        return false;
    }
    memset(resources.hostBuffer, 0, size);
    ret = aclrtMemcpy(resources.hostBuffer, size, resources.deviceBuffer, size, ACL_MEMCPY_DEVICE_TO_HOST);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Copy device buffer to host failed, ret:" << ret << ".";
        return false;
    }
    return VerifyPattern(resources.hostBuffer, size);
}

void Cleanup(mooncake::TransferEngine &engine, ProbeResources &resources)
{
    if (resources.hostRegistered) {
        int ret = engine.unregisterLocalMemory(resources.hostBuffer, true);
        if (ret != 0) {
            LOG(WARNING) << "Unregister host buffer failed, ret:" << ret << ".";
        }
    }
    if (resources.deviceBuffer != nullptr) {
        (void)aclrtFree(resources.deviceBuffer);
    }
    if (!resources.filePath.empty()) {
        (void)unlink(resources.filePath.c_str());
    }
    free(resources.hostBuffer);
    (void)engine.freeEngine();
    if (resources.context != nullptr) {
        (void)aclrtDestroyContext(resources.context);
    }
    (void)aclrtResetDevice(FLAGS_device_logic_id);
    (void)aclFinalize();
}

bool ValidateArguments()
{
    if (FLAGS_buffer_size == 0 || FLAGS_buffer_size % DIRECT_IO_ALIGNMENT != 0) {
        LOG(ERROR) << "Buffer size must be a non-zero multiple of " << DIRECT_IO_ALIGNMENT << ".";
        return false;
    }
    return FLAGS_rpc_port <= UINT16_MAX;
}
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    if (!ValidateArguments()) {
        return EXIT_FAILURE;
    }

    ProbeResources resources;
    if (posix_memalign(&resources.hostBuffer, DIRECT_IO_ALIGNMENT, FLAGS_buffer_size) != 0) {
        LOG(ERROR) << "Allocate aligned host buffer failed.";
        return EXIT_FAILURE;
    }
    FillPattern(resources.hostBuffer, FLAGS_buffer_size);
    if (!CreateProbeFile(resources, FLAGS_buffer_size)) {
        free(resources.hostBuffer);
        return EXIT_FAILURE;
    }

    aclError aclRet = aclInit(nullptr);
    if (aclRet != ACL_ERROR_NONE || aclrtSetDevice(FLAGS_device_logic_id) != ACL_ERROR_NONE ||
        aclrtCreateContext(&resources.context, FLAGS_device_logic_id) != ACL_ERROR_NONE) {
        LOG(ERROR) << "Initialize Ascend runtime failed, ret:" << aclRet << ".";
        (void)unlink(resources.filePath.c_str());
        free(resources.hostBuffer);
        return EXIT_FAILURE;
    }

    mooncake::TransferEngine engine(false);
    std::string localServer = FLAGS_local_ip + ":" + std::to_string(FLAGS_rpc_port);
#ifdef USE_ASCEND
    localServer += ":npu_" + std::to_string(FLAGS_device_phy_id);
#endif
    int ret = engine.init(FLAGS_metadata_server, localServer, FLAGS_local_ip, FLAGS_rpc_port);
    if (ret == 0) {
        ret = engine.registerLocalMemory(resources.hostBuffer, FLAGS_buffer_size, "cpu:0", false, true);
        resources.hostRegistered = ret == 0;
    }
    if (ret != 0) {
        LOG(ERROR) << "Register ordinary host buffer with TransferEngine failed, ret:" << ret << ".";
        Cleanup(engine, resources);
        return EXIT_FAILURE;
    }
    LOG(INFO) << "PASS: TransferEngine registered ordinary aligned host memory.";

    memset(resources.hostBuffer, 0, FLAGS_buffer_size);
    if (!ProbeDirectRead(resources, FLAGS_buffer_size) || !VerifyPattern(resources.hostBuffer, FLAGS_buffer_size)) {
        Cleanup(engine, resources);
        return EXIT_FAILURE;
    }
    LOG(INFO) << "PASS: O_DIRECT read into registered host memory succeeded.";

    if (!ProbeDeviceCopy(resources, FLAGS_buffer_size)) {
        Cleanup(engine, resources);
        return EXIT_FAILURE;
    }
    LOG(INFO) << "PASS: Registered host memory transferred to and from device memory.";
    Cleanup(engine, resources);
    return EXIT_SUCCESS;
}
