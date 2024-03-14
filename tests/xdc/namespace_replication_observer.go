package xdc

import (
	"context"
	"fmt"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests"
	"go.uber.org/fx"
)

func NewNamespaceReplicationObserver() *namespaceReplicationObserver {
	return &namespaceReplicationObserver{
		tasks: make(chan *replicationspb.NamespaceTaskAttributes, 100),
	}
}

type namespaceReplicationObserver struct {
	tasks chan *replicationspb.NamespaceTaskAttributes
}

type observableNamespaceReplicationTaskExecutor struct {
	tasks                   chan *replicationspb.NamespaceTaskAttributes
	replicationTaskExecutor namespace.ReplicationTaskExecutor
}

// Execute the replication task as-normal, but also send it to the channel so that the test can wait for it to
// know that the namespace data has been replicated.
func (t *observableNamespaceReplicationTaskExecutor) Execute(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	err := t.replicationTaskExecutor.Execute(ctx, task)
	if err != nil {
		return err
	}
	t.tasks <- task
	return nil
}

func (o *namespaceReplicationObserver) ServerOption() tests.Option {
	return tests.WithFxOptionsForService(primitives.WorkerService,
		fx.Decorate(
			func(executor namespace.ReplicationTaskExecutor) namespace.ReplicationTaskExecutor {
				return &observableNamespaceReplicationTaskExecutor{
					replicationTaskExecutor: executor,
					tasks:                   o.tasks,
				}
			},
		),
	)
}

func (o *namespaceReplicationObserver) BlockUntilReplicated(ctx context.Context, ns string) error {
	for {
		select {
		case task := <-o.tasks:
			if task.Info.Name == ns {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("%w: namespace %s", ctx.Err(), ns)
		}
	}
}
