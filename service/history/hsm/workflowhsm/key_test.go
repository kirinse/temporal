package workflowhsm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/hsm/workflowhsm"
)

func TestParseKey(t *testing.T) {
	t.Parallel()
	_, err := workflowhsm.ParseKey("test")
	require.Error(t, err)
	_, err = workflowhsm.ParseKey("test/test")
	require.Error(t, err)
	key, err := workflowhsm.ParseKey("nsID/wfID/runID")
	require.NoError(t, err)
	assert.Equal(t, definition.NewWorkflowKey("nsID", "wfID", "runID"), key.WorkflowKey)
	assert.Empty(t, key.Components)
	key, err = workflowhsm.ParseKey("nsID/wfID/runID/comp1/comp2")
	require.NoError(t, err)
	assert.Equal(t, definition.NewWorkflowKey("nsID", "wfID", "runID"), key.WorkflowKey)
	assert.Equal(t, []string{"comp1", "comp2"}, key.Components)
}
