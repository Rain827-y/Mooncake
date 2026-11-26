// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_MEMORY_ENGINE_CLIENT_H
#define TENT_MEMORY_ENGINE_CLIENT_H

#include <memory>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class Config;

class MemoryEngine {
   public:
    // The calling thread must already be bound to its Ascend device.
    static Status Create(const Config &config, std::unique_ptr<MemoryEngine> *engine);
    static Status Create(std::unique_ptr<MemoryEngine> *engine);
    ~MemoryEngine();

   private:
    struct Impl;
    explicit MemoryEngine(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> mImpl;
};

}
}

#endif
