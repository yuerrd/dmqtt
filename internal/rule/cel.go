package rule

import (
	"encoding/json"
	"fmt"

	"github.com/google/cel-go/cel"
)

// compiledRule holds a rule with its pre-compiled CEL program.
type compiledRule struct {
	Rule
	program cel.Program
}

// newCELEnv creates the CEL environment with variables available in filter expressions.
func newCELEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("payload", cel.DynType),
		cel.Variable("topic", cel.StringType),
		cel.Variable("qos", cel.IntType),
		cel.Variable("clientID", cel.StringType),
	)
}

// compileRule compiles a rule's CEL filter expression.
func compileRule(env *cel.Env, r Rule) (*compiledRule, error) {
	if r.Filter == "" {
		r.Filter = "true"
	}
	ast, issues := env.Compile(r.Filter)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile CEL expression %q: %w", r.Filter, issues.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program CEL expression %q: %w", r.Filter, err)
	}
	return &compiledRule{Rule: r, program: prg}, nil
}

// evaluate runs the compiled CEL program against the given activation variables.
// Returns true if the filter matches, false otherwise.
// Returns false (not error) if payload is not valid JSON.
func (cr *compiledRule) evaluate(topic string, payload []byte, qos byte, clientID string) (bool, error) {
	var payloadMap interface{}
	if err := json.Unmarshal(payload, &payloadMap); err != nil {
		return false, nil // non-JSON payload: skip rule, no error
	}

	activation := map[string]interface{}{
		"payload":  payloadMap,
		"topic":    topic,
		"qos":      int64(qos),
		"clientID": clientID,
	}

	out, _, err := cr.program.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("eval CEL: %w", err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL expression did not return bool, got %T", out.Value())
	}
	return result, nil
}
