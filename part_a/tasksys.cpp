#include <cstdio>

#include "CycleTimer.h"
#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    worker_threads = new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete [] worker_threads;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    task_counter = 0;
    for (int i = 0; i < num_threads; i++) {
        worker_threads[i] = std::thread(&TaskSystemParallelSpawn::runInBulk, this, runnable, num_total_tasks);
    }
    for (int i = 0; i < num_threads; i++) {
        worker_threads[i].join();
    }
}

void TaskSystemParallelSpawn::runInBulk(IRunnable* runnable, int num_total_tasks) {
    while (true) {
        int task_id = task_counter.fetch_add(1);

        // putting this check here instead of in loop condition prevents race conditions
        if (task_id >= num_total_tasks) {
            break;
        }

        runnable->runTask(task_id, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    thread_pool = new std::thread[num_threads];
    num_total_tasks = 0;
    {
        std::unique_lock<std::mutex> lock(mtx);
        terminate = false;
    }
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runInBulk, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::unique_lock<std::mutex> lock(mtx);
        terminate = true;
    }
    for (int i = 0; i < num_threads ; i++) {
        if (thread_pool[i].joinable()) {
            thread_pool[i].join();
        }
    }
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->runnable = runnable;    
    task_completed.store(0);
    task_counter.store(0);
    this->num_total_tasks = num_total_tasks;
    while (true) {
        if (task_completed.load() == num_total_tasks) {
            break;
        }
    }
    this->num_total_tasks = 0;
}

void TaskSystemParallelThreadPoolSpinning::runInBulk() {
    while (true) {
        while (num_total_tasks == 0 || task_counter.load() >= num_total_tasks) {
            std::unique_lock<std::mutex> lock(mtx);
            if (terminate) {
                return;
            }
        }
        int task_id = task_counter.fetch_add(1);
        if (task_id < num_total_tasks) {
            runnable->runTask(task_id, num_total_tasks);
            task_completed.fetch_add(1);
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    std::unique_lock<std::mutex> lock(mtx);
    this->num_threads = num_threads;
    thread_pool = new std::thread[num_threads];
    num_total_tasks = 0;
    task_counter.store(0);
    terminate = false;
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runInBulk, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    std::unique_lock<std::mutex> lock(mtx);
    terminate = true;
    cv.notify_all();
    lock.unlock();
    for (int i = 0; i < num_threads ; i++) {
        if (thread_pool[i].joinable()) {
            thread_pool[i].join();
        }
    }
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    {
        std::unique_lock<std::mutex> lock(mtx);
        this->runnable = runnable;    
        task_completed.store(0);
        task_counter.store(0);
        this->num_total_tasks = num_total_tasks;
    }
    
    cv.notify_all();

    std::unique_lock<std::mutex> lock(mtx);
    while (task_completed.load() < num_total_tasks) {
        cv2.wait(lock);
    }
}

void TaskSystemParallelThreadPoolSleeping::runInBulk() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [this] { return task_counter.load() < num_total_tasks || terminate; });
            if (terminate) return;
        }
        int task_id = task_counter.fetch_add(1);
        if (task_id < num_total_tasks) {
            runnable->runTask(task_id, num_total_tasks);
            if (task_completed.fetch_add(1) == num_total_tasks - 1) {
                std::unique_lock<std::mutex> lock(mtx);
                cv2.notify_one();
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
