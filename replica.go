package chord

import (
	"bytes"
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"math"
)

type ReplicaGroup struct {
	leaderId []byte
	data map[string][]byte
}

func (n *Node) addRgMembership(id uint64) {
	log.Infof("addRgMembership(%d)\n", id)
	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()

	_, ok := n.rgs[id]
	if ok {
		log.Errorf("addRgMembership(id) - RG for id already exists\n")
		return
	}

	n.rgs[id] = &ReplicaGroup{leaderId:Uint64ToBytes(id)}
	n.rgs[id].data = make(map[string][]byte)
	return
}

func (n *Node) removeRgMembership(id uint64) {
	log.Infof("removeRgMembership(%d)\n", id)
	n.rgsMtx.Lock()
	_, ok := n.rgs[id]
	if ok {
		delete(n.rgs, id)
	}
	n.rgsMtx.Unlock()
}

func (n *Node) removeFarthestRgMembership() {
	log.Infof("removeFarthestRgMembership()\n")
	n.rgsMtx.RLock()
	numRgs := len(n.rgs)
	n.rgsMtx.RUnlock()
	// Do not remove membership if we are not a part of the max
	// number of replica groups allowed -> len(successorList) + 1
	if numRgs < (n.config.SuccessorListSize + 1) {
		log.Infof("inside removeFarthestRgMembership - exiting since numRgs = %d\n", numRgs)
		return
	}

	// Remove farthest rg membership
	id := n.getFarthestRgMembership()
	n.removeRgMembership(id)
}

func (n *Node) getFarthestRgMembership() uint64 {
	log.Infof("getFarthestRgMembership()\n")
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

// TODO: cleanup  the below functions for sending/moving keys and replicas
func (n *Node) sendReplica(key string) {
	n.rgsMtx.RLock()
	defer n.rgsMtx.RUnlock()

	leaderID := BytesToUint64(n.Id)
	// get value for key
	val, ok := n.rgs[leaderID].data[key]
	if !ok {
		log.Errorf("sendReplica() exiting since key does not exist in our datastore\n")
	}
	// create kv
	kv := &chordpb.KV{Key:key, Value:val}
	// create replicaMsg
	replicaMsg := &chordpb.ReplicaMsg{LeaderId:n.Id, Kv:[]*chordpb.KV{kv}}


	// send kv to replica group
	n.succListMtx.RLock()
	succList := n.successorList
	n.succListMtx.RUnlock()
	for _, node := range succList {
		if bytes.Equal(node.Id, n.Id) {
			continue
		}
		n.SendReplicasRPC(node,replicaMsg)
	}
}

func (n *Node) sendAllReplicas() {
	n.rgsMtx.RLock()
	defer n.rgsMtx.RUnlock()

	leaderID := BytesToUint64(n.Id)

	if len(n.rgs[leaderID].data) == 0 {
		return
	}

	// Create kv array
	kvs := make([]*chordpb.KV, len(n.rgs[leaderID].data))
	index := 0
	for k, v := range n.rgs[leaderID].data {
		kvs[index] = &chordpb.KV{Key:k, Value:v}
		index++
	}

	// create replicaMsg
	replicaMsg := &chordpb.ReplicaMsg{LeaderId:n.Id, Kv: kvs}

	// send kvs to replica group
	n.succListMtx.RLock()
	succList := n.successorList
	n.succListMtx.RUnlock()
	for _, node := range succList {
		if bytes.Equal(node.Id, n.Id) {
			continue
		}
		n.SendReplicasRPC(node,replicaMsg)
	}
}

// strictly move new replicas to our RG
// will take care of sending new replicas outside this function
func (n *Node) moveReplicas(fromId uint64, toId uint64) {
	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()

	_ , ok := n.rgs[fromId]
	if !ok {
		log.Errorf("moveReplicas(from: %d, to: %d) exiting since fromId is not a current replica group leader\n", fromId, toId)
		return
	}

	_ , ok = n.rgs[toId]
	if !ok {
		log.Errorf("moveReplicas(from: %d, to: %d) exiting since toId is not a current replica group leader\n", fromId, toId)
		return
	}

	for k, v := range n.rgs[fromId].data {
		n.rgs[toId].data[k] = v
	}
	return
}

// move keys from our RG to a newly joined node
// remove the kvs from our data store
// return a list of kvs to be passed to sendKeys() so that
// the newly joined node receives its existing keys
func (n *Node) moveKeys(fromId []byte, toId []byte) []*chordpb.KV {
	fromId_uint := BytesToUint64(fromId)
	//toId_uint := BytesToUint64(toId)
	kvs := make([]*chordpb.KV, 0)

	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()

	var hash []byte
	for k, v := range n.rgs[fromId_uint].data {
		hash = GetPeerID(k, n.config.KeySize)
		if !BetweenRightIncl(hash, toId, fromId) {
			kvs = append(kvs, &chordpb.KV{Key:k, Value:v})
			// remove kv from our data store
			//delete(n.rgs[fromId_uint].data, k)
			// SEND REMOVE TO OUR RG
		}
	}
	return kvs

}

// Remove keys from fromId's replica group, if toId is responsible for them
func (n *Node) removeKeys(fromId []byte, toId []byte) []*chordpb.KV {
	fromId_uint := BytesToUint64(fromId)

	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()

	var hash []byte
	kvs := make([]*chordpb.KV, 0)
	for k, v := range n.rgs[fromId_uint].data {
		hash = GetPeerID(k, n.config.KeySize)
		if !BetweenRightIncl(hash, toId, fromId) {
			// remove kv from our data store
			delete(n.rgs[fromId_uint].data, k)
			// append to list tracking which keys we have removed
			kvs = append(kvs, &chordpb.KV{Key:k, Value:v})
		}
	}
	return kvs
}
