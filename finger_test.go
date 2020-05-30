package chord

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFingerMath(t *testing.T) {
	m := 8
	start := []byte{0}
	var key []byte
	var res int
	ans := [][]byte{{1}, {2}, {4}, {8}, {16}, {32}, {64}, {128}}

	for i := 0; i < m; i++ {
		key = fingerMath(start, i, m)
		res = bytes.Compare(key, ans[i])
		assert.Equal(t, res, 0, fmt.Sprintf("finger math incorrect for index %d", i))
	}

}
