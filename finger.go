package chord

import (
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"math/big"
)

type fingerTable []*fingerEntry

type fingerEntry struct {
	Id   []byte        // Id calculated by formula
	Node *chordpb.Node // Closest peer >= Id
}

/* Function: 	NewFingerTable
 *
 * Description:
 * 		Create a new finger table for a node. Initially all entries
 * 		will contain n.
 */
func NewFingerTable(n *Node, m int) fingerTable {
	ft := make([]*fingerEntry, m)

	// 节点 n 的路由表由 m 个路由项构成。
	n.ftMtx.Lock()
	for i := range ft {
		ft[i] = newFingerEntry(fingerMath(n.Id, i, m), n.Node)
	}
	n.ftMtx.Unlock()

	return ft
}

/* Function: 	newFingerEntry
 *
 * Description:
 * 		Return a newly allocated finger entry with the attributes set
 */
func newFingerEntry(id []byte, n *chordpb.Node) *fingerEntry {
	return &fingerEntry{
		Id:   id,
		Node: n,
	}
}

/* Function: 	fingerMath
 *
 * Description:
 * 		Calculate the new id in the chord ring based on the following formula:
 *		(n+2^i)mod(2^m)
 */
func fingerMath(n []byte, i int, m int) []byte {
	// 计算 2^i
	x := big.NewInt(2)
	x.Exp(x, big.NewInt(int64(i)), nil)

	// 计算 2^m
	y := big.NewInt(2)
	y.Exp(y, big.NewInt(int64(m)), nil)

	// 计算 (n + 2^i) mod 2^m
	res := &big.Int{}
	res.SetBytes(n).Add(res, x).Mod(res, y)

	// 返回
	return res.Bytes()
}

/* Function: 	fixFinger
 *
 * Description:
 * 		Fix a finger table entry if it is no longer correct.
 */
func (n *Node) fixFinger(next int) {
	// 计算节点 n 的第 next 个路由项的 CID
	nextID := fingerMath(n.Id, next, n.config.KeySize)

	// 查找路由表中距离 CID 最近的首个后继节点
	succ, err := n.findSuccessor(nextID)
	if err != nil {
		return
	}

	// 更新 nextID 路由项对应的 succ
	newEntry := newFingerEntry(nextID, succ)
	n.ftMtx.Lock()
	n.fingerTable[next] = newEntry
	n.ftMtx.Unlock()
}

/* Function: 	PrintFingerTable
 *
 * Description:
 * 		Print the entire finger table for a node. Can either print out the
 * 		node ids in hex or decimal.
 */
func (n *Node) PrintFingerTable(hex bool) {
	n.ftMtx.Lock()
	log.Printf("-----FINGER TABLE-----\n")
	ft := n.fingerTable
	for i, v := range ft {
		if hex {
			log.Infof("FT Entry %d - {id: %x, Node{id: %x, addr: %s, port: %d}}\n", i, v.Id, v.Node.Id, v.Node.Addr, v.Node.Port)
		} else {
			log.Infof("FT Entry %d - {id: %d, Node{id: %d, addr: %s, port: %d}}\n", i, v.Id, v.Node.Id, v.Node.Addr, v.Node.Port)
		}
	}
	n.ftMtx.Unlock()
}
