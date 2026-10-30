//
// Created by Shagu on 29.10.2026.
//

#ifndef SHUTTLE_ASYNC_I_CANCELLABLE_HPP
#define SHUTTLE_ASYNC_I_CANCELLABLE_HPP

namespace shuttle::async {
    struct ICancellable {
        virtual void cancel() = 0;
        virtual ~ICancellable() = default;
    };
}

#endif //SHUTTLE_ASYNC_I_CANCELLABLE_HPP
