package chord

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBetween(t *testing.T) {
	var res bool

	// a < b
	a := []byte{100}
	b := []byte{200}
	res = Between([]byte{150}, a, b)
	assert.True(t, res, "150 between (100, 200) should be true")

	res = Between([]byte{50}, a, b)
	assert.False(t, res, "50 between (100, 200) should be false")

	res = Between([]byte{100}, a, b)
	assert.False(t, res, "100 between (100, 200) should be false")

	res = Between([]byte{200}, a, b)
	assert.False(t, res, "200 between (100, 200) should be false")

	// a > b
	a = []byte{200}
	b = []byte{100}
	res = Between([]byte{250}, a, b)
	assert.True(t, res, "250 between (200, 100) should be true")

	res = Between([]byte{150}, a, b)
	assert.False(t, res, "150 between (200, 100) should be false")

	res = Between([]byte{200}, a, b)
	assert.False(t, res, "200 between (200, 100) should be false")

	res = Between([]byte{100}, a, b)
	assert.False(t, res, "100 between (200, 100) should be false")

	// a == b
	a = []byte{100}
	b = []byte{100}
	res = Between([]byte{250}, a, b)
	assert.True(t, res, "250 between (100, 100) should be true")

	res = Between([]byte{100}, a, b)
	assert.False(t, res, "100 between (100, 100) should be false")
}

func TestBetweenRightIncl(t *testing.T) {
	var res bool

	// a < b
	a := []byte{100}
	b := []byte{200}
	res = BetweenRightIncl([]byte{150}, a, b)
	assert.True(t, res, "150 between (100, 200] should be true")

	res = BetweenRightIncl([]byte{50}, a, b)
	assert.False(t, res, "50 between (100, 200] should be false")

	res = BetweenRightIncl([]byte{100}, a, b)
	assert.False(t, res, "100 between (100, 200] should be false")

	res = BetweenRightIncl([]byte{200}, a, b)
	assert.True(t, res, "200 between (100, 200] should be true")

	// a > b
	a = []byte{200}
	b = []byte{100}
	res = BetweenRightIncl([]byte{250}, a, b)
	assert.True(t, res, "250 between (200, 100] should be true")

	res = BetweenRightIncl([]byte{150}, a, b)
	assert.False(t, res, "150 between (200, 100] should be false")

	res = BetweenRightIncl([]byte{200}, a, b)
	assert.False(t, res, "200 between (200, 100] should be false")

	res = BetweenRightIncl([]byte{100}, a, b)
	assert.True(t, res, "100 between (200, 100] should be true")

	// a == b
	a = []byte{100}
	b = []byte{100}
	res = BetweenRightIncl([]byte{250}, a, b)
	assert.True(t, res, "250 between (100, 100] should be true")

	res = BetweenRightIncl([]byte{100}, a, b)
	assert.True(t, res, "100 between (100, 100] should be true")
}
