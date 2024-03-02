package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

type SyncHSMStateTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Version             int64
}

func (a *SyncHSMStateTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *SyncHSMStateTask) GetVersion() int64 {
	return a.Version
}

func (a *SyncHSMStateTask) SetVersion(version int64) {
	a.Version = version
}

func (a *SyncHSMStateTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *SyncHSMStateTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *SyncHSMStateTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *SyncHSMStateTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *SyncHSMStateTask) GetCategory() Category {
	return CategoryReplication
}

func (a *SyncHSMStateTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM_STATE
}
