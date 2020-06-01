package chord

import (
	"github.com/sirupsen/logrus"
	"math"
)

type ReplicaGroup struct {
	leaderId []byte
	data map[string][]byte
}

func (n *Node) addRgMembership(id uint64) {
	logrus.Infof("addRgMembership(%d)\n", id)
	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()

	_, ok := n.rgs[id]
	if ok {
		logrus.Errorf("addRgMembership(id) - RG for id already exists. This shouldn't happen!\n")
		return
	}

	n.rgs[id] = &ReplicaGroup{leaderId:Uint64ToBytes(id)}
	n.rgs[id].data = make(map[string][]byte)
	return
}

func (n *Node) removeRgMembership(id uint64) {
	logrus.Infof("removeRgMembership(%d)\n", id)
	n.rgsMtx.Lock()
	_, ok := n.rgs[id]
	if ok {
		delete(n.rgs, id)
	}
	n.rgsMtx.Unlock()
}

func (n *Node) removeFarthestRgMembership() {
	logrus.Infof("removeFarthestRgMembership()\n")
	n.rgsMtx.RLock()
	numRgs := len(n.rgs)
	n.rgsMtx.RUnlock()
	// Do not remove membership if we are not a part of the max
	// number of replica groups allowed -> len(successorList) + 1
	if numRgs < (n.config.SuccessorListSize + 1) {
		logrus.Infof("inside removeFarthestRgMembership - exiting since numRgs = %d\n", numRgs)
		return
	}

	// Remove farthest rg membership
	id := n.getFarthestRgMembership()
	n.removeRgMembership(id)
}

func (n *Node) getFarthestRgMembership() uint64 {
	logrus.Infof("getFarthestRgMembership()\n")
	n.rgsMtx.RLock()
	defer n.rgsMtx.RUnlock()
	// get ids for replica groups we are apart of
	keys := make([]uint64, len(n.rgs))
	i := 0
	for k := range n.rgs {
		keys[i] = k
		i++
	}

	var farthestId, maxDist, dist uint64
	ourId := BytesToUint64(n.Id)
	m := int(math.Pow(2.0, float64(n.config.KeySize)))

	for _, id := range keys {
		dist = Distance(ourId, id, m)
		if dist > maxDist {
			maxDist = dist
			farthestId = id
		}
	}

	return farthestId
}