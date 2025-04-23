//
// Created by Yufei on 2024/12/26.
//

#ifndef CONNECTIONPOOL_H
#define CONNECTIONPOOL_H

#ifdef WITH_TEE  

#include <iostream>
#include <vector>
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "TEE_connection.h"



// 连接池类
class ConnectionPool {
public:
    using ConnectionPtr = std::shared_ptr<TEE_connection>;

    // 初始化连接池
    ConnectionPool(size_t poolSize) : poolSize_(poolSize) {
        for (size_t i = 0; i < poolSize_; ++i) {
            auto conn = std::make_shared<TEE_connection>(i);
            conn->allocate_shared_menary_sssp();
            pool_.emplace(conn);
        }
    }

    // 获取连接
    ConnectionPtr acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return !pool_.empty(); });

        auto conn = pool_.front();
        pool_.pop();
        return conn;
    }

    // 归还连接
    void release(ConnectionPtr conn) {
        std::unique_lock<std::mutex> lock(mutex_);
        pool_.emplace(std::move(conn));
        cond_.notify_one();
    }

private:
    size_t poolSize_;
    std::queue<ConnectionPtr> pool_;
    std::mutex mutex_;
    std::condition_variable cond_;
};

#endif

#endif //CONNECTIONPOOL_H
