package chord

import (
	"bytes"
	"errors"
	"github.com/cdesiniotis/chord/chordpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Node implements the Chord GRPC Server interface
type Node struct {
	*chordpb.Node

	config *Config

	predecessor *chordpb.Node
	predMtx     sync.RWMutex

	successor *chordpb.Node
	succMtx   sync.RWMutex

	successorList []*chordpb.Node
	succListMtx   sync.RWMutex

	fingerTable fingerTable
	ftMtx       sync.RWMutex

	sock       *net.TCPListener
	grpcServer *grpc.Server
	grpcOpts   grpcOpts

	connPool    map[string]*clientConn
	connPoolMtx sync.RWMutex

	data    map[string][]byte
	dataMtx sync.RWMutex

	signalChannel chan os.Signal
	shutdownCh    chan struct{}
}

// Some constants for readability
var (
	emptyNode = &chordpb.Node{}
)

/* Function: 	CreateChord
 *
 * Description:
 * 		Create a new Chord ring and return the first node
 *		in the ring.
 */
func CreateChord(config *Config) *Node {
	n := newNode(config)
	n.create()
	return n
}

/* Function: 	JoinChord
 *
 * Description:
 * 		Join an existing Chord ring. addr and port specify
 * 		an existing node in the Chord ring. Returns a newly
 * 		created node with its successor set.
 */
func JoinChord(config *Config, addr string, port int) (*Node, error) {
	n := newNode(config)
	err := n.join(&chordpb.Node{Addr: addr, Port: uint32(port)})
	if err != nil {
		log.Errorf("error joining existing chord ring: %v\n", err)
		n.shutdown()
		return nil, err
	}
	return n, err
}

/* Function: 	newNode
 *
 * Description:
 * 		Create and initialize a new node based on the config.yaml.
 * 		Start all of the necessary threads required by the
 * 		Chord protocol.
 */
func newNode(config *Config) *Node {
	// Set timestamp format for the logger
	log.SetFormatter(&log.TextFormatter{TimestampFormat: "2006-01-02 15:04:05", FullTimestamp: true})

	// Initialize some attributes
	n := &Node{
		Node:          &chordpb.Node{Addr: config.Addr, Port: config.Port},
		config:        config,
		successorList: make([]*chordpb.Node, config.SuccessorListSize),
		connPool:      make(map[string]*clientConn),
		grpcOpts: grpcOpts{
			serverOpts: config.ServerOpts,
			dialOpts:   config.DialOpts,
			timeout:    time.Duration(config.Timeout) * time.Millisecond},
		data:          make(map[string][]byte),
		shutdownCh:    make(chan struct{}),
		signalChannel: make(chan os.Signal, 1),
	}

	// Get PeerID
	key := n.Addr + ":" + strconv.Itoa(int(n.Port))
	n.Id = GetPeerID(key, config.KeySize)

	// Create new finger table
	n.fingerTable = NewFingerTable(n, config.KeySize)

	// Create a listening socket for the chord grpc server
	lis, err := net.Listen("tcp", key)
	if err != nil {
		log.Fatalf("error creating listening socket %v\n", err)
	}
	n.sock = lis.(*net.TCPListener)

	// Create and register the chord grpc Server
	n.grpcServer = grpc.NewServer()
	chordpb.RegisterChordServer(n.grpcServer, n)

	// Thread 1: gRPC Server
	go func() {
		err := n.grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("error bringing up grpc server: %s\n", err)
		}
	}()

	log.Infof("Server is listening on %v\n", key)

	// Thread 2: Catch registered signals
	signal.Notify(n.signalChannel,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-n.signalChannel
		n.shutdown()
		os.Exit(0)
	}()

	// Thread 3: Debug
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				log.Printf("------------\n")
				PrintNode(n.Node, false, "Self")
				PrintNode(n.predecessor, false, "Predecessor")
				PrintNode(n.successor, false, "Successor")
				PrintSuccessorList(n)
				n.PrintFingerTable(false)
				log.Printf("------------\n")
			case <-n.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Thread 4: Stabilization protocol
	go func() {
		ticker := time.NewTicker(time.Duration(n.config.StabilizeInterval) * time.Millisecond)
		for {
			select {
			case <-ticker.C:

				n.stabilize()
			case <-n.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Thread 5: Fix Finger Table periodically
	go func() {
		next := 0
		ticker := time.NewTicker(time.Duration(n.config.FixFingerInterval) * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				n.fixFinger(next)
				next = (next + 1) % n.config.KeySize
			case <-n.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Thread 6: Check health status of predecessor
	go func() {
		ticker := time.NewTicker(time.Duration(n.config.CheckPredecessorInterval) * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				n.checkPredecessor()
			case <-n.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	return n
}

/*
 * Function:	shutdown
 *
 * Description:
 *		Gracefully shutdown a node by performing some cleanup.
 */
func (n *Node) shutdown() {
	log.Infof("In shutdown()\n")
	close(n.shutdownCh)

	log.Infof("Closing grpc server...\n")
	n.grpcServer.Stop()

	n.connPoolMtx.Lock()
	for addr, cc := range n.connPool {
		log.Infof("Closing conn %v for addr %v\n", cc, addr)
		cc.conn.Close()
		n.connPool[addr] = nil
	}
	n.connPoolMtx.Unlock()

	log.Infof("Closing listening socket\n")
	n.sock.Close()
}

/*
 * Function:	create
 *
 * Description:
 *		Create a new Chord ring. Set our predecessor to nil.
 * 		Set the successor pointer to point to ourselves.
 */
func (n *Node) create() {
	n.predMtx.Lock()
	n.predecessor = nil
	n.predMtx.Unlock()

	n.succMtx.Lock()
	n.successor = n.Node
	n.succMtx.Unlock()

	n.initSuccessorList()
}

/*
 * Function:	join
 *
 * Description:
 *		Join an existing Chord ring. Set our predecessor to nil,
 * 		but ask a node to find us our successor. "Other" is the
 *		node we know of when joining the Chord ring.
 */
func (n *Node) join(other *chordpb.Node) error {
	n.predMtx.Lock()
	n.predecessor = nil
	n.predMtx.Unlock()

	succ, err := n.FindSuccessorRPC(other, n.Id)
	if err != nil {
		log.Errorf("error calling FindSuccessorRPC(): %s\n", err)
		return err
	}

	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	return nil
}

/*
 * Function:	stabilize
 *
 * Description:
 *		Implementation of the Chord stabilization protocol. Update our successor
 * 		pointer if a new node has joined between us and successor. Notify
 * 		out successor that we believe we are its predecessor.
 */
func (n *Node) stabilize() {
	/* PSEUDOCODE from paper
	x = successor.predecessor
	if (x ∈ (n, successor)) {
		successor = x
	}
	successor.notify(n)
	*/

	// MY MODIFICATIONS
	n.updateSuccessorList()
	// ---------------

	// Must have a successor first prior to running stabilization
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	// Don't do anything is successor isn't set
	if succ == nil {
		return
	}

	// TODO: handle when successor fails
	// Get our successors predecessor
	x, err := n.GetPredecessorRPC(succ)
	if err != nil || x == nil {
		log.Errorf("error invoking GetPredecessorRPC: %s\n", err)
		return
	}

	// Update our successor if a new node joined between
	// us and our current successor
	if x.Id != nil && Between(x.Id, n.Id, succ.Id) {
		log.Infof("stabilize(): updating our successor to - %v\n", x)
		n.succMtx.Lock()
		n.successor = x
		n.succMtx.Unlock()
	}

	// Notify our successor of our existence
	n.succMtx.RLock()
	succ = n.successor
	n.succMtx.RUnlock()

	_ = n.NotifyRPC(succ)
}

/*
 * Function:	updateSuccessorList
 *
 * Description:
 *		An addition to the Chord stabilization protocol. Update the successor list
 * 		periodically. If n notices its successor has failed, it will replace
 * 		its successor with the next entry in the successor list and reconcile its
 * 		list with its new successor.
 */
func (n *Node) updateSuccessorList() {
	var succ *chordpb.Node

	index := 0
	for {
		n.succMtx.RLock()
		succ = n.successor
		n.succMtx.RUnlock()

		succList, err := n.GetSuccessorListRPC(succ)
		if err != nil {
			log.Errorf("successor failed while calling GetSuccessorListRPC: %v\n", err)
			// update successor the next entry in successor table
			n.succMtx.Lock()
			n.succListMtx.RLock()
			n.successor = n.successorList[index+1]
			n.succListMtx.RUnlock()
			n.succMtx.Unlock()
			index++
		} else {
			n.reconcileSuccessorList(succList)
			break
		}
	}

}

/*
 * Function:	reconcileSuccessorList
 *
 * Description:
 *		Node n reconciles its list with successor s by copying s's list,
 * 		removing the last element, and prepending s to it.
 */
func (n *Node) reconcileSuccessorList(succList *chordpb.SuccessorList) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	// Remove succList's last element
	list := succList.Successors
	copy(list[1:], list)
	// Prepend our successor to the list
	list[0] = succ

	// Update our successor list
	n.succListMtx.Lock()
	n.successorList = list
	n.succListMtx.Unlock()
}

/*
 * Function:	findSuccessor
 *
 * Description:
 *		Find the successor node for the given id. First check if id ∈ (n, successor].
 *		If this is not the case then forward the request to the closest preceding node.
 */
func (n *Node) findSuccessor(id []byte) (*chordpb.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	if BetweenRightIncl(id, n.Id, succ.Id) {
		return succ, nil
	} else {
		n2 := n.closestPrecedingNode(id)
		res, err := n.FindSuccessorRPC(n2, id)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
}

/*
 * Function:	closestPrecedingNode
 *
 * Description:
 *		Check finger table and find closest preceding node for a given id.
 */
func (n *Node) closestPrecedingNode(id []byte) *chordpb.Node {
	n.ftMtx.RLock()
	defer n.ftMtx.RUnlock()

	for i := len(id) - 1; i >= 0; i-- {
		if Between(n.fingerTable[i].Id, n.Id, id) {
			ret := n.fingerTable[i].Node
			return ret
		}
	}
	return n.Node
}

/*
 * Function:	checkPredecessor
 *
 * Description:
 *		Check whether the current predecessor is still alive
 */
func (n *Node) checkPredecessor() {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if pred == nil {
		return
	}

	_, err := n.CheckPredecessorRPC(pred)
	if err != nil {
		log.Infof("detected predecessor has failed\n")
		n.predMtx.Lock()
		n.predecessor = nil
		n.predMtx.Unlock()
	}
}

/*
 * Function:	initSuccessorList
 *
 * Description:
 *		Initialize values of successor list to current successor. Only
 * 		used by creator of chord ring.
 */
func (n *Node) initSuccessorList() {
	n.succMtx.Lock()
	for i, _ := range n.successorList {
		n.successorList[i] = n.successor
	}
	n.succMtx.Unlock()
}

/*
 * Function:	get
 *
 * Description:
 *		Get a key's value in the datastore. First locate which
 * 		node in the ring is responsible for the key, then call
 *		GetRPC if the node is remote.
 */
func (n *Node) get(key string) ([]byte, error) {
	node, err := n.locate(key)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(n.Id, node.Id) == 0 {
		// key is stored at current node
		n.dataMtx.RLock()
		val, ok := n.data[key]
		n.dataMtx.RUnlock()

		if !ok {
			return nil, errors.New("key does not exist in datastore")
		}

		return val, nil
	} else {
		// key is stored at a remote node
		val, err := n.GetRPC(node, key)
		if err != nil {
			log.Errorf("error getting a key from a remote node: %s", err)
			return nil, err
		}
		return val.Value, nil
	}

}

/*
 * Function:	put
 *
 * Description:
 *		Put a key-value in the datastore. First locate which
 * 		node in the ring is responsible for the key, then call
 *		PutRPC if the node is remote.
 */
func (n *Node) put(key string, value []byte) error {
	node, err := n.locate(key)
	if err != nil {
		return err
	}

	if bytes.Compare(n.Id, node.Id) == 0 {
		// key belongs to current node
		n.dataMtx.Lock()
		n.data[key] = value
		n.dataMtx.Unlock()
		return nil
	} else {
		// key belongs to remote node
		_, err := n.PutRPC(node, key, value)
		return err
	}
}

/*
 * Function:	locate
 *
 * Description:
 *		Locate which node in the ring is responsible for a key.
 */
func (n *Node) locate(key string) (*chordpb.Node, error) {
	hash := GetPeerID(key, n.config.KeySize)
	node, err := n.findSuccessor(hash)
	if err != nil || node == nil {
		log.Errorf("error locating node storing the key %s with hash %d\n", key, hash)
		return nil, errors.New("error finding node storing key")
	}
	return node, nil
}
