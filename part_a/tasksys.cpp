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
    task_done = true;
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runInBulk, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    for (int i = 0; i < num_threads; i++) {
        if (thread_pool[i].joinable()) {
            thread_pool[i].join();
        }
    }
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    task_counter.store(0);
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    task_done.store(false);
    while (!task_done.load()) {}
    // barrier
}

void TaskSystemParallelThreadPoolSpinning::runInBulk() {
    while (true) {
        while (task_done.load()) {}

        int task_id = task_counter.fetch_add(1);

        if (task_id >= num_total_tasks) {
            task_done.store(true);
            // barrier
        } else {
            runnable->runTask(task_id, num_total_tasks);
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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
