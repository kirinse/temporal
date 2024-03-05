package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

type SyncHSMTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Version             int64
}

func (a *SyncHSMTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *SyncHSMTask) GetVersion() int64 {
	return a.Version
}

func (a *SyncHSMTask) SetVersion(version int64) {
	a.Version = version
}

func (a *SyncHSMTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *SyncHSMTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *SyncHSMTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *SyncHSMTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *SyncHSMTask) GetCategory() Category {
	return CategoryReplication
}

func (a *SyncHSMTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM
}
