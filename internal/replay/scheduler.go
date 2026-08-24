package replay

import (
	"sort"
	"sync"
	"time"
)

// TaskState 描述回放任务的流转状态。
type TaskState string

const (
	TaskPending  TaskState = "pending"
	TaskRunning  TaskState = "running"
	TaskComplete TaskState = "complete"
	TaskResumed  TaskState = "resumed"
)

// Task 是一条回放任务的运行时记录。
type Task struct {
	ID         string
	State      TaskState
	StartedAt  time.Time
	FinishedAt time.Time
	Packets    int
}

// TaskTracker 维护任务状态机，监控页面展示其流转历史。
type TaskTracker struct {
	mu    sync.Mutex
	tasks map[string]*Task
	order []string
}

// NewTaskTracker 构造任务跟踪器。
func NewTaskTracker() *TaskTracker {
	return &TaskTracker{tasks: make(map[string]*Task)}
}

// Begin 登记一条任务并把状态置为 pending 后转 running。
func (t *TaskTracker) Begin(id string) *Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	task := &Task{ID: id, State: TaskPending, StartedAt: time.Now().UTC()}
	t.tasks[id] = task
	t.order = append(t.order, id)
	task.State = TaskRunning
	return task
}

// Complete 把任务标记为完成。
func (t *TaskTracker) Complete(id string, packets int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	task := t.tasks[id]
	if task == nil {
		return
	}
	task.State = TaskComplete
	task.Packets = packets
	task.FinishedAt = time.Now().UTC()
}

// MarkResumed 把任务标记为从游标恢复。
func (t *TaskTracker) MarkResumed(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	task := t.tasks[id]
	if task == nil {
		return
	}
	task.State = TaskResumed
}

// Snapshot 返回任务的排序快照。
func (t *TaskTracker) Snapshot() []Task {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Task, 0, len(t.tasks))
	for _, id := range t.order {
		if task := t.tasks[id]; task != nil {
			out = append(out, *task)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out
}
