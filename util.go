package chord

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"math"
	"math/big"
	"strconv"
)

/* Function:	GetHash
 *
 * Description:
 *		Given an input string return the SHA-1 hash.
 */
func GetHash(key string) []byte {
	h := sha1.New()
	_, err := h.Write([]byte(key))
	if err != nil {
		return nil
	}
	bs := h.Sum(nil)
	return bs
}

/* Function:	GetPeerID
 *
 * Description:
 *		Given an input string (usually ip:port), return
 * 		the peer ID. The peer ID is a SHA-1 hash truncated
 * 		to m bits. There are 2^m -1 possible peer IDs.
 * 		m must be a multiple of 8.
 */
func GetPeerID(key string, m int) []byte {

	if m%8 != 0 {
		log.Fatalf("GetPeerID(): m is not a multiple of 8\n")
	}

	hash := GetHash(key)
	str := hex.EncodeToString(hash)

	numHexChars := m / 4
	var id []byte
	err := error(nil)
	id, err = hex.DecodeString(str[:numHexChars])
	if err != nil {
		return nil
	}

	return id
}

/* Function:	GetLocationOnRing
 *
 * Description:
 *		Given an input id, return the location on the
 * 		Chord ring as a percent. Useful for debugging or
 * 		testing purposes. Assumes id is a multiple of 8 bits.
 */
func GetLocationOnRing(id []byte) float64 {
	str := hex.EncodeToString(id)
	m := len(str)
	max := math.Pow(2.0, float64(m*4)) - 1

	idInt, _ := strconv.ParseUint(str, 16, 64)
	return (float64(idInt) / max) * 100.0
}

/* Function:	BetweenRightIncl
 *
 * Description:
 *		Check if a key is between a and b, right inclusive.
 */
func BetweenRightIncl(key, a, b []byte) bool {
	return Between(key, a, b) || bytes.Equal(key, b)
}

/* Function:	Between
 *
 * Description:
 *		Check if a key is strictly between a and b.
 */
func Between(key, a, b []byte) bool {
	switch bytes.Compare(a, b) {
	case 1:
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) > 0
	case -1:
		return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) > 0
	case 0:
		return bytes.Compare(a, key) != 0
	}
	return false
}

/* Function:	Contains
 *
 * Description:
 *		Returns true if node is in list.
 */
func Contains(list []*chordpb.Node, node *chordpb.Node) bool {
	for _, n := range list {
		if bytes.Equal(n.Id, node.Id) {
			return true
		}
	}
	return false
}

/* Function:	PrintNode
 *
 * Description:
 *		Print basic info about a chordpb.Node to stdout. Can either print out
 * 		the node's id in hex or decimal.
 */
func PrintNode(n *chordpb.Node, hex bool, label string) {
	if n == nil {
		//fmt.Printf("%s - nil\n", label)
		log.Infof("%s - nil", label)
		return
	}

	if hex {
		log.Infof("%s - {id: %x\t addr: %s\t port: %d}\n", label, n.Id, n.Addr, n.Port)
	} else {
		log.Infof("%s - {id: %d\t addr: %s\t port: %d}\n", label, n.Id, n.Addr, n.Port)
	}

}

/* Function:	PrintSuccessorList
 *
 * Description:
 *		Print out a node's successor list
 *
 */
func PrintSuccessorList(n *Node) {
	n.succListMtx.RLock()
	defer n.succListMtx.RUnlock()

	for i, node := range n.successorList {
		PrintNode(node, false, fmt.Sprintf("Successor %d", i))
	}
}

func PrintReplicaGroupMembership(n *Node) {
	n.rgsMtx.RLock()
	defer n.rgsMtx.RUnlock()

	log.Infof("------Replica Group Membership------\n")
	for id, _ := range n.rgs {
		log.Infof("RG Leader ID: %d\t RG data: %v\n", id, n.rgs[id].data)
	}
}

func BytesToUint64(b []byte) uint64 {
	temp := big.Int{}
	return temp.SetBytes(b).Uint64()
}

func Uint64ToBytes(i uint64) []byte {
	temp := big.Int{}
	return temp.SetUint64(i).Bytes()
}

func Distance(a, b uint64, n int) uint64 {
	sub := float64(a-b)
	_n := float64(n)
	return uint64(math.Min(math.Abs(sub), float64(_n-math.Abs(sub))))
}

// returns true if same, false if different
func CompareSuccessorLists(a, b []*chordpb.Node) bool {
	for i, _ := range a {
		if !bytes.Equal(a[i].Id, b[i].Id) {
			return false
		}
	}
	return true
}