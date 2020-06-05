package chord

import (
	"github.com/cdesiniotis/chord/chordpb"
	"github.com/stretchr/testify/assert"
	"math"
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

func TestDistance(t *testing.T) {
	var a, b, res uint64
	n := 8

	a = 255
	b = 30

	res = Distance(a, b, int(math.Pow(2.0, float64(n))))
	assert.Equal(t, 31, int(res), "Distance(255,30,256) should be 31")

	a = 255
	b = 0
	res = Distance(a, b, int(math.Pow(2.0, float64(n))))
	assert.Equal(t, 1, int(res), "Distance(255,0,256) should be 1")

}

func TestCompareSuccessorLists(t *testing.T) {
	var res bool

	a := []*chordpb.Node{ {Id:[]byte{69}}, {Id:[]byte{118}}}
	b := []*chordpb.Node{ {Id:[]byte{69}}, {Id:[]byte{118}}}
	res = CompareSuccessorLists(a, b)
	assert.True(t, res, "comparing both successor lists should result in true")

	a = []*chordpb.Node{ {Id:[]byte{69}}, {Id:[]byte{118}}}
	b = []*chordpb.Node{ {Id:[]byte{69}}, {Id:[]byte{119}}}
	res = CompareSuccessorLists(a, b)
	assert.False(t, res, "comparing both successor lists should result in false")
}