package remit

import (
	. "github.com/tj/go-debug"
)

var debug = Debug("remit")

// J is a convenient aliaas for a `map[string]interface{}`, useful for dealing with
// JSON is a more native manner.
//
// 	remit.J{
// 		"foo": "bar",
// 		"baz": true,
// 		"qux": remit.J{
// 			"big": false,
// 			"small": true,
// 		},
// 	}
//
type J map[string]interface{}
