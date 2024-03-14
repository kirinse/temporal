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

package xdc

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/tests"
)

type (
	nexusRequestForwardingSuite struct {
		xdcBaseSuite
		activeClusterName            string
		passiveClusterName           string
		namespace                    string
		namespaceReplicationObserver *namespaceReplicationObserver
	}
)

func TestNexusRequestForwardingSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(nexusRequestForwardingSuite))
}

func (s *nexusRequestForwardingSuite) SetupSuite() {
	s.activeClusterName = "nexus-request-forwarding-test-active"
	s.passiveClusterName = "nexus-request-forwarding-test-passive"
	s.namespace = "nexus-request-forwarding-test-namespace"
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableNexusHTTPHandler: true,
	}
	s.namespaceReplicationObserver = NewNamespaceReplicationObserver()
	s.setupSuite([]string{s.activeClusterName, s.passiveClusterName}, s.namespaceReplicationObserver.ServerOption())
}

func (s *nexusRequestForwardingSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *nexusRequestForwardingSuite) SetupTest() {
	s.setupTest()
}

func (s *nexusRequestForwardingSuite) TearDownTest() {
}

func (s *nexusRequestForwardingSuite) TestNexusRequestForwarding() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second*debug.TimeoutMultiplier)
	defer cancel()
	s.registerNamespace(ctx, s.namespace)
	taskQueue := "test-task-queue"
	baseURL := fmt.Sprintf(
		"http://%s/%s/",
		s.cluster1.GetHost().FrontendHTTPAddress(),
		cnexus.Routes().DispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{
			Namespace: s.namespace,
			TaskQueue: taskQueue,
		}),
	)
	client, err := nexus.NewClient(nexus.ClientOptions{
		ServiceBaseURL: baseURL,
	})
	s.NoError(err)
	s.NotNil(client)
	frontendClient := s.cluster1.GetFrontendClient()
	identity := "test-identity"
	errs := make(chan error, 1)
	go func() {
		errs <- s.runPoller(ctx, frontendClient, identity, taskQueue)
	}()
	time.Sleep(1 * time.Second)
	operation, err := client.StartOperation(ctx, "test-operation", "World", nexus.StartOperationOptions{})
	s.NoError(err)
	err = <-errs
	s.NoError(err)
	var result string
	err = operation.Successful.Consume(&result)
	s.NoError(err)
	s.Equal("Hello, World!", result)
}

func (s *nexusRequestForwardingSuite) runPoller(ctx context.Context, frontendClient tests.FrontendClient, identity string, taskQueue string) error {
	response, err := frontendClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.namespace,
		Identity:  identity,
		TaskQueue: &taskqueuepb.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			Name: taskQueue,
		},
	})
	if err != nil {
		return err
	}
	startOperationRequest := response.Request.GetStartOperation()
	var input string
	dataConverter := converter.GetDefaultDataConverter()
	err = dataConverter.FromPayload(startOperationRequest.Payload, &input)
	s.NoError(err)
	reply := "Hello, " + input + "!"
	payload, err := dataConverter.ToPayload(reply)
	s.NoError(err)
	_, err = frontendClient.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.namespace,
		Identity:  identity,
		TaskToken: response.TaskToken,
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{
							Payload: payload,
						},
					},
				},
			},
		},
	})
	return err
}
