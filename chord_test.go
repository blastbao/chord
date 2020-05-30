package chord

import (
	"bytes"
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var n1, n2, n3 *Node

//var n1ID, n2ID, n3ID []byte

// check if successors are set correctly
func TestSuccessor(t *testing.T) {
	var res int

	assert.NotNil(t, n1.successor, "n1's successor should not be nil")
	if n1.successor != nil {
		res = bytes.Compare(n1.successor.Id, []byte{19})
		assert.Equal(t, 0, res, "n1's successor should be n3")
	}

	assert.NotNil(t, n2.successor, "n2's successor should not be nil")
	if n2.successor != nil {
		res = bytes.Compare(n2.successor.Id, []byte{118})
		assert.Equal(t, 0, res, "n2's successor should be n1")
	}

	assert.NotNil(t, n3.successor, "n3's successor should not be nil")
	if n3.successor != nil {
		res = bytes.Compare(n3.successor.Id, []byte{69})
		assert.Equal(t, 0, res, "n3's successor should be n2")
	}
}

// check if predecessors are set correctly
func TestPredecessor(t *testing.T) {
	var res int

	assert.NotNil(t, n1.predecessor, "n1's predecessor should not be nil")
	if n1.predecessor != nil {
		res = bytes.Compare(n1.predecessor.Id, []byte{69})
		assert.Equal(t, 0, res, "n1's predecessor should be n2")
	}

	assert.NotNil(t, n2.predecessor, "n2's predecessor should not be nil")
	if n2.predecessor != nil {
		res = bytes.Compare(n2.predecessor.Id, []byte{19})
		assert.Equal(t, 0, res, "n2's predecessor should be n3")
	}

	assert.NotNil(t, n3.predecessor, "n3's predecessor should not be nil")
	if n3.predecessor != nil {
		res = bytes.Compare(n3.predecessor.Id, []byte{118})
		assert.Equal(t, 0, res, "n3's predecessor should be n1")
	}
}

// check if findSuccessor works correctly
func TestFindSuccessor(t *testing.T) {
	var res int
	var node *chordpb.Node

	node, _ = n1.findSuccessor([]byte{118})
	assert.NotNil(t, node, "n1.findSuccessor(118) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{118})
		assert.Equal(t, 0, res, "n1.findSuccessor(118) should return 118")
	}

	node, _ = n1.findSuccessor([]byte{119})
	assert.NotNil(t, node, "n1.findSuccessor(119) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{19})
		assert.Equal(t, 0, res, "n1.findSuccessor(119) should return 19")
	}

	node, _ = n1.findSuccessor([]byte{19})
	assert.NotNil(t, node, "n1.findSuccessor(19) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{19})
		assert.Equal(t, 0, res, "n1.findSuccessor(19) should return 19")
	}

	node, _ = n1.findSuccessor([]byte{20})
	assert.NotNil(t, node, "n1.findSuccessor(20) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{69})
		assert.Equal(t, 0, res, "n1.findSuccessor(20) should return 69")
	}

	node, _ = n1.findSuccessor([]byte{69})
	assert.NotNil(t, node, "n1.findSuccessor(69) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{69})
		assert.Equal(t, 0, res, "n1.findSuccessor(69) should return 69")
	}

	node, _ = n1.findSuccessor([]byte{70})
	assert.NotNil(t, node, "n1.findSuccessor(70) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{118})
		assert.Equal(t, 0, res, "n1.findSuccessor(70) should return 118")
	}

	node, _ = n1.findSuccessor([]byte{59})
	assert.NotNil(t, node, "n1.findSuccessor(59) should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{69})
		assert.Equal(t, 0, res, "n1.findSuccessor(59) should return 69")
	}

}

func TestSuccessorList(t *testing.T) {
	var res int
	var node *chordpb.Node
	list1 := n1.successorList
	list2 := n2.successorList
	list3 := n3.successorList

	node = list1[0]
	assert.NotNil(t, node, "n1.successorList[0] should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{19})
		assert.Equal(t, 0, res, "n1.successorList[0] should return 19")
	}

	node = list1[1]
	assert.NotNil(t, node, "n1.successorList[1] should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{69})
		assert.Equal(t, 0, res, "nn1.successorList[1] should return 69")
	}

	node = list2[0]
	assert.NotNil(t, node, "n2.successorList[0] should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{118})
		assert.Equal(t, 0, res, "n2.successorList[0] should return 118")
	}

	node = list2[1]
	assert.NotNil(t, node, "n2.successorList[1] should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{19})
		assert.Equal(t, 0, res, "n2.successorList[1] should return 19")
	}

	node = list3[0]
	assert.NotNil(t, node, "n3.successorList[0] should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{69})
		assert.Equal(t, 0, res, "n3.successorList[0] should return 69")
	}

	node = list3[1]
	assert.NotNil(t, node, "n3.successorList[1] should not return nil")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{118})
		assert.Equal(t, 0, res, "n3.successorList[1] should return 118")
	}
}

func TestLocate(t *testing.T) {
	var res int
	var err error
	var node *chordpb.Node

	//hashSize := n1.config.yaml.KeySize

	key1 := "key1"
	key2 := "key2"
	key3 := "key3"

	//hash1 := GetPeerID(key1, hashSize)
	//hash2 := GetPeerID(key2, hashSize)
	//hash3 := GetPeerID(key3, hashSize)

	/*
	 * key - key1	 hash - [16]	 locate - [19]
	 * key - key2	 hash - [135]	 locate - [19]
	 * key - key3	 hash - [59]	 locate - [69]
	 */
	//t.Logf("key - %s\t hash - %d", key1, hash1)
	//t.Logf("key - %s\t hash - %d", key2, hash2)
	//t.Logf("key - %s\t hash - %d", key3, hash3)

	node, err = n1.locate(key1)
	assert.Nil(t, err, "locate(k) should not result in error")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{19})
		assert.Equalf(t, 0, res, "n1.locate(%s) should return [19]\n", key1)
	}
	//t.Logf("key - %s\t hash - %d\t locate - %d", key1, hash1, node.Id)

	node, err = n1.locate(key2)
	assert.Nil(t, err, "locate(k) should not result in error")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{19})
		assert.Equalf(t, 0, res, "n1.locate(%s) should return [19]\n", key2)
	}
	//t.Logf("key - %s\t hash - %d\t locate - %d", key2, hash2, node.Id)

	node, err = n1.locate(key3)
	assert.Nil(t, err, "locate(k) should not result in error")
	if node != nil {
		res = bytes.Compare(node.Id, []byte{69})
		assert.Equalf(t, 0, res, "n1.locate(%s) should return [69]\n", key3)
	}

}

func TestPut(t *testing.T) {
	//var res int
	var err error
	//var node *chordpb.Node

	key1 := "key1"
	key2 := "key2"
	key3 := "key3"

	err = n1.put(key1, []byte("val1"))
	assert.Nil(t, err, "put(k,v) should not result in error")
	err = n1.put(key2, []byte("val2"))
	assert.Nil(t, err, "put(k,v) should not result in error")
	err = n1.put(key3, []byte("val3"))
	assert.Nil(t, err, "put(k,v) should not result in error")
}

func TestGet(t *testing.T) {
	var res int
	var val []byte
	var err error

	key1 := "key1"
	key2 := "key2"
	key3 := "key3"

	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")

	val, err = n1.get(key1)
	assert.Nil(t, err, "get(k) should not result in error")
	res = bytes.Compare(val, val1)
	assert.Equalf(t, 0, res, "n1.get(%s) should return %s\n", key1, string(val1))

	val, err = n1.get(key2)
	assert.Nil(t, err, "get(k) should not result in error")
	res = bytes.Compare(val, val2)
	assert.Equalf(t, 0, res, "n1.get(%s) should return %s\n", key2, string(val2))

	val, err = n1.get(key3)
	assert.Nil(t, err, "get(k) should not result in error")
	res = bytes.Compare(val, val3)
	assert.Equalf(t, 0, res, "n1.get(%s) should return %s\n", key3, string(val3))

	val, err = n1.get("key4")
	assert.NotNil(t, err, "get(k) should result in error for key not present in datastore")
}

func TestMain(m *testing.M) {
	var err error
	// Create a few sample nodes
	// Node 1 with ID: [118]
	cfg := DefaultConfig("0.0.0.0", 8001)
	n1 = CreateChord(cfg)
	// Node 2 with ID: [69]
	cfg = DefaultConfig("0.0.0.0", 8002)
	n2, err = JoinChord(cfg, "0.0.0.0", 8001)
	if err != nil {
		log.Errorf("Exiting in TestMain()\n")
		n1.shutdown()
		n2.shutdown()
		os.Exit(1)
	}
	// Node 3 with ID: [19]
	cfg = DefaultConfig("0.0.0.0", 8003)
	n3, err = JoinChord(cfg, "0.0.0.0", 8001)
	if err != nil {
		log.Errorf("Exiting in TestMain()\n")
		n1.shutdown()
		n2.shutdown()
		n3.shutdown()
		os.Exit(1)
	}

	// Sleep for a few seconds so that nodes stabilize and converge
	time.Sleep(5 * time.Second)

	// Run tests
	exitStatus := m.Run()

	/* DEBUG
	PrintNode(n1.Node, false, "n1")
	PrintNode(n1.predecessor, false, "n1 pred")
	PrintNode(n1.successor, false, "n1 succ")
	n1.PrintFingerTable(false)
	PrintNode(n2.Node, false, "n2")
	PrintNode(n2.predecessor, false, "n2 pred")
	PrintNode(n2.successor, false, "n2 succ")
	n2.PrintFingerTable(false)
	PrintNode(n3.Node, false, "n3")
	PrintNode(n3.predecessor, false, "n3 pred")
	PrintNode(n3.successor, false, "n3 succ")
	n3.PrintFingerTable(false)
	*/

	// Cleanup
	n1.shutdown()
	n2.shutdown()
	n3.shutdown()

	// Exit
	os.Exit(exitStatus)
}
