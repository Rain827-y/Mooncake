// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_MEMORY_ENGINE_CLIENT_H
#define TENT_MEMORY_ENGINE_CLIENT_H

#include <cstddef>
#include <memory>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class Config;

enum class MemoryEngineMemType { Host, Device };

class MemoryEngine {
   public:
    // The calling thread must already be bound to its Ascend device.
    static Status Create(const Config &config, std::unique_ptr<MemoryEngine> *engine);
    static Status Create(std::unique_ptr<MemoryEngine> *engine);
    ~MemoryEngine();

    Status Join();
    // Quiesce any direct access to GVA before leaving.
    Status Leave();
    Status Allocate(size_t size, MemoryEngineMemType type, void **address);
    // Stop direct access on all participants before freeing the region.
    Status Free(void *address);

   private:
    struct Impl;
    explicit MemoryEngine(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> mImpl;
};

}
}

#endif
