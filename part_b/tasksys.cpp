#include "tasksys.h"
#include "cstdio"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    num_launches = 0;
    std::unique_lock<std::mutex> lock(mtx);
    this->num_threads = num_threads;
    thread_pool = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runInBulk, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    for (Launch* launch : launches) {
        delete launch;
    }
}

void TaskSystemParallelThreadPoolSleeping::topologicalSort(TaskID launch_id) {
    if (visited[launch_id]) {
        return;
    }

    visited[launch_id] = true;
    for (TaskID child : children[launch_id]) {
        topologicalSort(child);
    }

    sorted_launches.push(launch_id);
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    task_counters.push_back(0);
    task_completed.push_back(0);
    printf("Constructing graph with launch id: %d\n", num_launches);
    launches.push_back(new Launch{runnable, num_total_tasks, deps});
    visited.push_back(false);
    children.push_back(std::vector<TaskID>());
    for (TaskID dep : deps) {
        printf("Adding child %d to launch %d\n", dep, num_launches);
        children[dep].push_back(num_launches);
    }
    printf("Finished constructing graph with launch id: %d\n", num_launches);
    return num_launches++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    for (TaskID i = 0; i < num_launches; i++) {
        topologicalSort(i);
    }
    // print sorted_launches
    printf("----------Sorted launches----------\n");
    while (!sorted_launches.empty()) {
        TaskID launch_id = sorted_launches.top();
        printf("[Info] Launch ID: %d\n", launch_id);
        Launch *launch = launches[launch_id];
        printf("[Info] Number of tasks: %d\n", launch->num_total_tasks);
        printf("[Info] Dependencies: ");
        for (TaskID dep : launch->deps) {
            printf("%d ", dep);
        }
        printf("\n\n");
        sorted_launches.pop();
    }

    return;
}

void TaskSystemParallelThreadPoolSleeping::runInBulk() {
    while (true) {
        // cv from sync to start
        // add termination flag

        std::unique_lock<std::mutex> lock(mtx);
        if (!working_launches.empty()) {
            TaskID launchID = working_launches[0];
            Launch *launch = launches[launchID];
            int task_counter = task_counters[launchID];
            task_counters[launchID]++;
            int num_total_tasks = launch->num_total_tasks;
            IRunnable *runnable = launch->runnable;
            if (task_counter < num_total_tasks) {
                lock.unlock();
                runnable->runTask(task_counter, num_total_tasks);
                lock.lock();
                task_completed[launchID]++;
            } else {
                // remove the specific launchID in working_launches
                working_launches.erase(std::remove(working_launches.begin(), working_launches.end(), launchID), working_launches.end());
            }

        } else {
            int new_launch_id = sorted_launches.top();
            bool ready = true;
            for (TaskID dep : launches[new_launch_id]->deps) {
                if (task_completed[dep] != launches[dep]->num_total_tasks) {
                    ready = false;
                    break;
                }
            }
            if (ready) {
                working_launches.push_back(new_launch_id);
                sorted_launches.pop();
            } else {
                cv.wait(lock);
            }
        }
    }
}