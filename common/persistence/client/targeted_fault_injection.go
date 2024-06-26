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

package client

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
)

// NewTargetedDataStoreErrorGenerator returns a new instance of a data store error generator that will inject errors
// into the persistence layer based on the provided configuration.
func NewTargetedDataStoreErrorGenerator(cfg *config.FaultInjectionDataStoreConfig) ErrorGenerator {
	methods := make(map[string]ErrorGenerator, len(cfg.Methods))
	for methodName, methodConfig := range cfg.Methods {
		var faultWeights []FaultWeight
		methodErrRate := 0.0
		for errName, errRate := range methodConfig.Errors {
			err := newError(errName, errRate, methodName)
			faultWeights = append(faultWeights, FaultWeight{
				errFactory: func(data string) *fault {
					return err
				},
				weight: errRate,
			})
			methodErrRate += errRate
		}
		errGenerator := NewDefaultErrorGenerator(methodErrRate, faultWeights)
		seed := methodConfig.Seed
		if seed == 0 {
			seed = time.Now().UnixNano()
		}
		errGenerator.r = rand.New(rand.NewSource(seed))
		methods[methodName] = errGenerator
	}
	return &dataStoreErrorGenerator{MethodErrorGenerators: methods}
}

// dataStoreErrorGenerator is an implementation of ErrorGenerator that will inject errors into the persistence layer
// using a per-method configuration.
type dataStoreErrorGenerator struct {
	MethodErrorGenerators map[string]ErrorGenerator
}

// Generate returns an error from the configured error types and rates for this method.
// This method infers the fault injection target's method name from the function name of the caller.
// As a result, this method should only be called from the persistence layer.
// This method will panic if the method name cannot be inferred.
// If no errors are configured for the method, or if there are some errors configured for this method,
// but no error is sampled, then this method returns nil.
// When this method returns nil, this causes the persistence layer to use the real implementation.
func (d *dataStoreErrorGenerator) Generate() *fault {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		panic("failed to get caller info")
	}
	runtimeFunc := runtime.FuncForPC(pc)
	if runtimeFunc == nil {
		panic("failed to get runtime function")
	}
	parts := strings.Split(runtimeFunc.Name(), ".")
	methodName := parts[len(parts)-1]
	methodErrorGenerator, ok := d.MethodErrorGenerators[methodName]
	if !ok {
		return nil
	}
	return methodErrorGenerator.Generate()
}

// newError returns an error based on the provided name. If the name is not recognized, then this method will
// panic.

type fault struct {
	err error
	// execOp indicates whether the operation should be executed before returning the error.
	execOp bool
}

func newFault(err error) *fault {
	return &fault{
		err:    err,
		execOp: false,
	}
}
func inject0(f *fault, op func() error) error {
	if f == nil {
		return op()
	}
	if f.execOp {
		err := op()
		if err != nil {
			return err
		}
	}
	return f.err
}

func inject1[T1 any](f *fault, op func() (T1, error)) (T1, error) {
	if f == nil {
		return op()
	}
	if f.execOp {
		r1, err := op()
		if err != nil {
			return r1, err
		}
	}
	var nilT1 T1
	return nilT1, f.err
}
func inject2[T1 any, T2 any](f *fault, op func() (T1, T2, error)) (T1, T2, error) {
	if f == nil {
		return op()
	}
	if f.execOp {
		r1, r2, err := op()
		if err != nil {
			return r1, r2, err
		}
	}
	var nilT1 T1
	var nilT2 T2
	return nilT1, nilT2, f.err
}

func newError(errName string, errRate float64, methodName string) *fault {
	header := fmt.Sprintf("fault injection error at %s with %.2f rate", methodName, errRate)
	switch errName {
	case "ShardOwnershipLost":
		return newFault(&persistence.ShardOwnershipLostError{Msg: fmt.Sprintf("%s: persistence.ShardOwnershipLostError", header)})
	case "DeadlineExceeded":
		// Real persistence store never returns context.DeadlineExceeded error. It returns persistence.TimeoutError instead.
		// Therefor "DeadlineExceeded" shouldn't be used with fault injection. Use "Timeout" instead.
		return newFault(fmt.Errorf("%s: %w", header, context.DeadlineExceeded))
	case "Timeout":
		return newFault(&persistence.TimeoutError{Msg: fmt.Sprintf("%s: persistence.TimeoutError", header)})
	case "ExecuteAndTimeout":
		// Special error which emulates case, when client got a Timeout error,
		// but operation actually reached persistence and was executed successfully.
		f := newFault(&persistence.TimeoutError{Msg: fmt.Sprintf("%s: persistence.TimeoutError", header)})
		f.execOp = true
		return f
	case "ResourceExhausted":
		return newFault(&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: fmt.Sprintf("%s: serviceerror.ResourceExhausted", header),
		})
	case "Unavailable":
		return newFault(serviceerror.NewUnavailable(fmt.Sprintf("%s: serviceerror.Unavailable", header)))
	default:
		panic(fmt.Sprintf("unsupported error type: %v", errName))
	}
}

// UpdateRate should not be called for the data store error generator since the rate is defined on a per-method basis.
func (d *dataStoreErrorGenerator) UpdateRate(rate float64) {
	panic("UpdateRate not supported for data store error generators")
}

// UpdateWeights should not be called for the data store error generator since the weights are defined on a per-method
// basis.
func (d *dataStoreErrorGenerator) UpdateWeights(weights []FaultWeight) {
	panic("UpdateWeights not supported for data store error generators")
}

// Rate should not be called for the data store error generator since there is no global rate for the data store, only
// per-method rates.
func (d *dataStoreErrorGenerator) Rate() float64 {
	panic("Rate not supported for data store error generators")
}
