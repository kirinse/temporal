// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package replication

import (
	"time"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableHSMStateTask struct {
		ProcessToolBox
		ExecutableTask
		task *replicationspb.SyncHSMTaskAttributes
	}
)

var _ ctasks.Task = (*ExecutableHSMStateTask)(nil)
var _ TrackableExecutableTask = (*ExecutableHSMStateTask)(nil)
var _ BatchableTask = (*ExecutableHSMStateTask)(nil)

func NewExecutableHSMStateTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncHSMTaskAttributes,
	sourceClusterName string,
) *ExecutableHSMStateTask {
	return &ExecutableHSMStateTask{
		ProcessToolBox: processToolBox,
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncHSMTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
		),
		task: task,
	}
}

func (e *ExecutableHSMStateTask) QueueID() interface{} {
	panic("implement me")
}

func (e *ExecutableHSMStateTask) Execute() error {
	if e.TerminalState() {
		return nil
	}
	// TODO: Add execution logic
	return nil
}

func (e *ExecutableHSMStateTask) HandleErr(err error) error {
	// TODO: Add error handling
	return err
}

func (e *ExecutableHSMStateTask) MarkPoisonPill() error {
	// TODO: Add poison pill handling
	panic("implement me")
}

func (e *ExecutableHSMStateTask) BatchWith(incomingTask BatchableTask) (TrackableExecutableTask, bool) {
	// TODO: Add batching
	return nil, false
}

func (e *ExecutableHSMStateTask) CanBatch() bool {
	return false
}

func (e *ExecutableHSMStateTask) MarkUnbatchable() {
}
