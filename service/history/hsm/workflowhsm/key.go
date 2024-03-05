package workflowhsm

import (
	"fmt"
	"strings"

	"go.temporal.io/server/common/definition"
)

type Key struct {
	definition.WorkflowKey
	Components []string
}

// ParseKey parses the key string as a list of components separated by forward slash delimiters. The first three
// components are used to create a WorkflowKey, and the rest are used to create a list of components.
func ParseKey(key string) (*Key, error) {
	components := strings.Split(key, "/")
	if len(components) < 3 {
		return nil, fmt.Errorf("not enough components to form workflow key from HSM key: %s", key)
	}
	return &Key{
		WorkflowKey: definition.NewWorkflowKey(components[0], components[1], components[2]),
		Components:  components[3:],
	}, nil
}
