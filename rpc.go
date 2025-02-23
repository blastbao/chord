package chord

import (
	"bytes"
	"context"
	"errors"
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"strconv"
	"time"
)

type grpcOpts struct {
	serverOpts []grpc.ServerOption
	dialOpts   []grpc.DialOption
	timeout    time.Duration
}

type clientConn struct {
	client chordpb.ChordClient
	conn   *grpc.ClientConn
}

/* Function: 	getChordClient
 *
 * Description:
 *		Returns a client necessary to make a chord grpc call.
 * 		Adds the client to the node's connection pool.
 */
//
// 获取同 other 的 grpc client
func (n *Node) getChordClient(other *chordpb.Node) (chordpb.ChordClient, error) {

	// 目标节点的地址
	target := other.Addr + ":" + strconv.Itoa(int(other.Port))

	// 连接池，grpc 可以复用连接
	n.connPoolMtx.RLock()
	cc, ok := n.connPool[target]
	n.connPoolMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	// 新建 grpc connection
	ctx, cancel := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, target, n.grpcOpts.dialOpts...)
	//conn, err := grpc.Dial(target, n.grpcOpts.dialOpts...)
	if err != nil {
		return nil, err
	}

	// 封装 grpc client
	client := chordpb.NewChordClient(conn)

	// 保存到连接池
	cc = &clientConn{client, conn}
	n.connPoolMtx.Lock()
	defer n.connPoolMtx.Unlock()
	if n.connPool == nil {
		return nil, errors.New("must instantiate node before using")
	}
	n.connPool[target] = cc

	return client, nil
}

/* Function: 	removeChordClient
 *
 * Description:
 *		Removes a stale chord client from connection pool
 */
func (n *Node) removeChordClient(other *chordpb.Node) {
	target := other.Addr + ":" + strconv.Itoa(int(other.Port))
	n.connPoolMtx.RLock()
	defer n.connPoolMtx.RUnlock()
	_, ok := n.connPool[target]
	if ok {
		delete(n.connPool, target)
	}
	return
}

/* Function: 	FindSuccessorRPC
 *
 * Description:
 *		Invoke a FindSuccessor RPC on node "other," asking for the successor of a given id.
 */
func (n *Node) FindSuccessorRPC(other *chordpb.Node, id []byte) (*chordpb.Node, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.PeerID{Id: id}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.FindSuccessor(ctx, req)
	return resp, err
}

/* Function: 	GetPredecessorRPC
 *
 * Description:
 *		Invoke a GetPredecessor RPC on node "other," asking for it's current predecessor.
 */
func (n *Node) GetPredecessorRPC(other *chordpb.Node) (*chordpb.Node, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.Empty{}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.GetPredecessor(ctx, req)
	return resp, err
}

/* Function: 	NotifyRPC
 *
 * Description:
 *		Invoke a Notify RPC on node "other," telling it that we believe we are its predecessor
 */
func (n *Node) NotifyRPC(other *chordpb.Node) error {

	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return err
	}
	req := n.Node

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	_, err = client.Notify(ctx, req)
	return err
}

/* Function: 	CheckPredecessorRPC
 *
 * Description:
 *		Invoke a CheckPredecessor RPC on node "other," asking if that node is still alive
 */
func (n *Node) CheckPredecessorRPC(other *chordpb.Node) (*chordpb.Empty, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.Empty{}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.CheckPredecessor(ctx, req)
	return resp, err
}

/* Function: 	GetSuccessorListRPC
 *
 * Description:
 *		Get another node's successor list
 */
func (n *Node) GetSuccessorListRPC(other *chordpb.Node) (*chordpb.SuccessorList, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.Empty{}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.GetSuccessorList(ctx, req)
	return resp, err
}

func (n *Node) RecvCoordinatorMsgRPC(other *chordpb.Node, newLeaderId []byte, oldLeaderId []byte) (error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return err
	}
	req := &chordpb.CoordinatorMsg{NewLeaderId:newLeaderId, OldLeaderId:oldLeaderId}

	// TODO: consider not sending with timeout here
	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	_, err = client.RecvCoordinatorMsg(ctx, req)
	return err
}

func (n *Node) GetKeysRPC(other *chordpb.Node, id []byte) (*chordpb.KVs, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.PeerID{Id:id}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp , err := client.GetKeys(ctx, req)
	return resp, err
}

func (n *Node) SendReplicasRPC(other *chordpb.Node, req *chordpb.ReplicaMsg) (error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return err
	}

	// TODO: consider not sending with timeout here
	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	_, err = client.SendReplicas(ctx, req)
	return err
}

func (n *Node) RemoveReplicasRPC(other *chordpb.Node, req *chordpb.ReplicaMsg) (error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return err
	}

	// TODO: consider not sending with timeout here
	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	_, err = client.RemoveReplicas(ctx, req)
	return err
}

func (n *Node) GetRPC(other *chordpb.Node, key string) (*chordpb.Value, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.Key{Key: key}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.Get(ctx, req)
	return resp, err
}

func (n *Node) PutRPC(other *chordpb.Node, key string, value []byte) (*chordpb.Empty, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.KV{Key: key, Value: value}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.Put(ctx, req)
	return resp, err
}

func (n *Node) LocateRPC(other *chordpb.Node, key string) (*chordpb.Node, error) {
	client, err := n.getChordClient(other)
	if err != nil {
		log.Errorf("error getting Chord Client: %v", err)
		return nil, err
	}
	req := &chordpb.Key{Key: key}

	ctx, _ := context.WithTimeout(context.Background(), n.grpcOpts.timeout)
	resp, err := client.Locate(ctx, req)
	return resp, err
}

/* Function: 	FindSuccessor
 *
 * Description:
 * 		Implementation of FindSuccessor RPC. Returns the successor of peerID.
 * 		If peerID is between our id and our successor's id, then return our successor.
 * 		Otherwise, check our finger table and forward the request to the closest preceding node.
 */
//
// [重要] 查找 peerID 的后继，递归查询，不断转发。类似 DNS 。
func (n *Node) FindSuccessor(context context.Context, peerID *chordpb.PeerID) (*chordpb.Node, error) {
	return n.findSuccessor(peerID.Id)
}

/* Function: 	GetPredecessor
 *
 * Description:
 * 		Implementation of GetPredecessor RPC. Returns the node's current predecessor
 */
//
// 获取前驱。
func (n *Node) GetPredecessor(context context.Context, empty *chordpb.Empty) (*chordpb.Node, error) {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	if n.predecessor == nil {
		return emptyNode, nil
	}
	return n.predecessor, nil
}

/* Function: 	Notify
 *
 * Description:
 * 		Implementation of Notify RPC. A Node is notifying us that it believes it is our predecessor.
 * 		Check if this is true based on our predecessor/successor knowledge and update.
 */
//
// 当其它节点通知自己，其为自己的前驱，那么需要更新本节点的 pred 。
func (n *Node) Notify(context context.Context, node *chordpb.Node) (*chordpb.Empty, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()

	// [重要] 检查 node.Id 位于 <pred, curr> 之间，其才是合法的最近前驱。
	if n.predecessor == nil || Between(node.Id, n.predecessor.Id, n.Id) {
		log.Infof("Notify(): Updating predecessor to: %v\n", node)
		n.predecessor = node
	}

	return &chordpb.Empty{}, nil
}

/* Function: 	CheckPredecessor
 *
 * Description:
 * 		Implementation of CheckPredecessor RPC. Simply return an empty response, confirming
 * 		the liveliness of a node.
 */
func (n *Node) CheckPredecessor(context context.Context, empty *chordpb.Empty) (*chordpb.Empty, error) {
	return &chordpb.Empty{}, nil
}

/* Function: 	GetSuccessorList
 *
 * Description:
 *		Return a node's successor list
 */
// 返回字节的后继列表。
func (n *Node) GetSuccessorList(context context.Context, empty *chordpb.Empty) (*chordpb.SuccessorList, error) {
	n.succListMtx.RLock()
	defer n.succListMtx.RUnlock()
	return &chordpb.SuccessorList{Successors: n.successorList}, nil
}

/* Function: 	ReceiveCoordinatorMsg
 *
 * Description:
 * 		Implementation of RecvCoordinatorMsg RPC. Other nodes will send us coordinator messages.
 *		This is a modified form of the Bully algorithm for leader election. When we receive a
 * 		coordinator msg from another node it means we are a member of their successor list.
 * 		We immediately recognize that we are apart of its replica group and take the necessary actions,
 * 		like creating a new replica group object internally, removing replica groups we are
 * 		no longer a member of etc.
 */

// RecvCoordinatorMsg
//
// 这是领导者选举的 Bully 算法的一种修改形式。
//
// 当我们收到另一个节点的协调消息时，意味着我们是其 successor list 中的成员。
// 我们应该立即认识到自己是其复制组的一部分，并采取必要的行动，如:
//	- 创建一个新的 replica group
//  - 删除一个旧的 replica group
//
func (n *Node) RecvCoordinatorMsg(context context.Context, msg *chordpb.CoordinatorMsg) (*chordpb.Empty, error) {

	// 如果自己是 leader ，忽略。
	if bytes.Equal(n.Id, msg.NewLeaderId) {
		return &chordpb.Empty{}, nil
	}

	log.Infof("ReceivedCoordinatorMsg(): newLeaderID: %d\t oldLeaderID: %d\n", msg.NewLeaderId, msg.OldLeaderId)

	// 如果旧 leader 不存在
	if len(msg.OldLeaderId) == 0 {
		// New node has joined chord ring

		// Check if this is a duplicate message
		// 检查新 leader 是否已经存在
		n.rgsMtx.RLock()
		_, ok := n.rgs[BytesToUint64(msg.NewLeaderId)]
		if ok {
			n.rgsMtx.RUnlock()
			return &chordpb.Empty{}, errors.New("received duplicate coordinator message")
		}
		n.rgsMtx.RUnlock()


		// Remove farthest RG membership
		//
		// 检查 n.rgs 是否超过限制，若超过需要移除一个，腾个空间。
		n.removeFarthestRgMembership()

		// Add new RG
		//
		// 添加副本
		newLeaderId := BytesToUint64(msg.NewLeaderId)
		n.addRgMembership(newLeaderId)


		// If newleader should be our predecessor, or is already our predecessor,
		// remove keys we are not responsible for anymore.
		// This new node already requested these keys from us when it joined the chord ring.
		//
		// 如果 newleader 应该是我们的前任，或者已经是我们的前任，则删除我们不再负责的键。
		// 这个新节点在加入和弦环时已经向我们请求了这些键。
		//
		n.predMtx.RLock()

		if n.predecessor == nil || Between(msg.NewLeaderId, n.predecessor.Id, n.Id) || bytes.Equal(msg.NewLeaderId, n.predecessor.Id) {

			// remove keys we aren't responsible for anymore
			//
			// 因为 leader 是我们的 pred ，所以位于 <pred, curr> 之间的 key 不归我们负责，需要删除他们。
			kvs := n.removeKeys(n.Id, msg.NewLeaderId)

			// remove these keys from our replica group
			//
			// 这些 kv 需要从自己的 replicas 中移除
			n.succListMtx.RLock()
			succList := n.successorList
			n.succListMtx.RUnlock()
			for _, node := range succList {
				n.RemoveReplicasRPC(node, &chordpb.ReplicaMsg{LeaderId:n.Id, Kv:kvs})
			}
		}


		n.predMtx.RUnlock()

	// 如果旧 leader 存在
	} else {

		newLeaderId := BytesToUint64(msg.NewLeaderId)
		oldLeaderId := BytesToUint64(msg.OldLeaderId)

		// Check if new leader or old leader is currently the leader
		// for a replica group we are a part of
		n.rgsMtx.RLock()
		_, newLeaderExists := n.rgs[newLeaderId]
		_, oldLeaderExists := n.rgs[oldLeaderId]
		n.rgsMtx.RUnlock()

		// Two cases where our replica group membership changes
		if newLeaderExists && oldLeaderExists {
			if (newLeaderId == oldLeaderId) {
				// RG membership has not changed - we are already in this RG
				return &chordpb.Empty{}, nil
			}
			// RG membership has changed, remove old leader
			n.removeRgMembership(oldLeaderId)
		} else if !newLeaderExists {
			if oldLeaderExists {
				// remove old leader which has presumably failed
				n.removeRgMembership(oldLeaderId)
			}
			// RG membership has changed, add new leader
			n.addRgMembership(newLeaderId)
		}

	}

	return &chordpb.Empty{}, nil
}

/* Function: 	GetKeys
 *
 * Description:
 * 		Implementation of GetKeys RPC. The caller of this RPC is requesting keys for which it
 * 		it responsible for along the chord ring. We simply check our own datastore for keys
 * 		that the other node is responsible for and send it.
 */
func (n *Node) GetKeys(context context.Context, id *chordpb.PeerID) (*chordpb.KVs, error) {
	n.rgsMtx.RLock()
	defer n.rgsMtx.RUnlock()

	ourId := BytesToUint64(n.Id)

	// 本地数据为空，直接返回。
	if len(n.rgs[ourId].data) == 0 {
		return &chordpb.KVs{}, nil
	}

	// [重要][数据迁移] 把本地数据中，那些 hash(key) 位于 (~, peer) 区间的 keys 返回给他，其余 <peer, curr> 区间的 keys 保留在本地。
	kvs := make([]*chordpb.KV, 0)
	var hash []byte
	for k, v := range n.rgs[ourId].data {
		hash = GetPeerID(k, n.config.KeySize)
		// TODO: ensure this only sends the necessary keys at all times
		if !BetweenRightIncl(hash, id.Id, n.Id) {
			kvs = append(kvs, &chordpb.KV{Key:k, Value:v})
		}
	}
	return &chordpb.KVs{Kvs:kvs}, nil
}

/* Function: 	SendReplicas
 *
 * Description:
 * 		Implementation of SendReplicas RPC. A leader is sending us kv replicas. Add them to the leaders
 * 		replica group internally.
 */
//
// 当某个 leader 发送 kvs 给本机，需要将数据保存到本地存储中。
func (n *Node) SendReplicas(context context.Context, replicaMsg *chordpb.ReplicaMsg) (*chordpb.Empty, error) {

	// 检查 leader 是否存在，若存在，意味者本机是一个副本。
	leaderId := BytesToUint64(replicaMsg.LeaderId)
	n.rgsMtx.RLock()
	_ , ok := n.rgs[leaderId]
	n.rgsMtx.RUnlock()
	if !ok {
		log.Errorf("SendReplicas() for leaderId %d, but not currently apart of this replica group\n", leaderId)
		return &chordpb.Empty{}, errors.New("node is not in replica group")
	}

	// 把 leader 发来的副本数据保存到本地
	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()
	for _ ,kv := range replicaMsg.Kv {
		n.rgs[leaderId].data[kv.Key] = kv.Value
	}

	return &chordpb.Empty{}, nil
}

/* Function: 	RemoveReplicas
 *
 * Description:
 * 		Implementation of RemoveReplicas RPC. A leader is informing us that certain keys do not belong
 * 		in this replica group anymore. Remove the specified keys from the leaders replica group internally
 */
func (n *Node) RemoveReplicas(context context.Context, replicaMsg *chordpb.ReplicaMsg) (*chordpb.Empty, error) {
	leaderId := BytesToUint64(replicaMsg.LeaderId)

	n.rgsMtx.RLock()
	_ , ok := n.rgs[leaderId]
	n.rgsMtx.RUnlock()

	if !ok {
		log.Errorf("RemoveReplicas() for leaderId %d, but not currently apart of this replica group\n", leaderId)
		return &chordpb.Empty{}, errors.New("node is not in replica group")
	}

	n.rgsMtx.Lock()
	defer n.rgsMtx.Unlock()
	for _ ,kv := range replicaMsg.Kv {
		delete(n.rgs[leaderId].data, kv.Key)
	}

	return &chordpb.Empty{}, nil
}

/* Function: 	Get
 *
 * Description:
 * 		Implementation of Get RPC.
 */
func (n *Node) Get(context context.Context, key *chordpb.Key) (*chordpb.Value, error) {
	val, err := n.get(key.Key)
	if err != nil {
		return nil, err
	}

	return &chordpb.Value{Value: val}, nil
}

/* Function: 	Put
 *
 * Description:
 * 		Implementation of Put RPC.
 */
func (n *Node) Put(context context.Context, kv *chordpb.KV) (*chordpb.Empty, error) {
	err := n.put(kv.Key, kv.Value)
	return &chordpb.Empty{}, err
}

/* Function: 	Locate
 *
 * Description:
 * 		Implementation of Locate RPC.
 */
func (n *Node) Locate(context context.Context, key *chordpb.Key) (*chordpb.Node, error) {
	return n.locate(key.Key)
}
