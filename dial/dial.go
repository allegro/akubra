package dial

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type watchConn struct {
	net.Conn
	closeCallback func(net.Conn, error)
}

func (wc *watchConn) Close() error {
	err := wc.Conn.Close()
	if wc.closeCallback != nil {
		wc.closeCallback(wc.Conn, err)
	}
	return err
}

// LimitDialer limits open connections by read and dial timeout. Also provides hard
// limit on number of open connections
type LimitDialer struct {
	activeCons      map[string]int64
	limit           int64
	dialTimeout     time.Duration
	readTimeout     time.Duration
	droppedEndpoint string
	countersMx      sync.Mutex
}

// ErrSlowOrMaintained is returned if LimitDialer exceeds connection limit
var ErrSlowOrMaintained = fmt.Errorf("Slow or maintained endpoint")

func (d *LimitDialer) incrementCount(addr string) (int64, error) {
	d.countersMx.Lock()
	defer d.countersMx.Unlock()
	_, ok := d.activeCons[addr]
	if !ok {
		d.activeCons[addr] = 0
	}

	d.activeCons[addr]++
	if d.limitReached(addr) {
		d.activeCons[addr]--
		return d.activeCons[addr], ErrSlowOrMaintained
	}

	return d.activeCons[addr], nil
}

func (d *LimitDialer) decrementCount(addr string) {
	d.countersMx.Lock()
	defer d.countersMx.Unlock()
	d.activeCons[addr]--
}

// checks if limit is reached and if given endpoint is most occupied
func (d *LimitDialer) limitReached(endpoint string) bool {
	numOfAllConns := int64(0)
	maxNumOfEndpointConns := int64(0)
	mostLoadedEndpoint := ""
	if endpoint == d.droppedEndpoint {
		return true
	}
	for key, count := range d.activeCons {
		numOfAllConns += count
		if count > maxNumOfEndpointConns {
			maxNumOfEndpointConns = count
			mostLoadedEndpoint = key
		}
	}
	if numOfAllConns > d.limit {
		return mostLoadedEndpoint == endpoint
	}
	return false
}

// Dial connects to endpoint as net.Dial does, but also keeps track
// on number of connections
func (d *LimitDialer) Dial(network, addr string) (c net.Conn, err error) {
	_, incErr := d.incrementCount(addr)
	if incErr != nil {
		return nil, incErr
	}

	var netconn net.Conn

	if d.dialTimeout > 0 {
		netconn, err = net.DialTimeout(network, addr, d.dialTimeout)
	} else {
		netconn, err = net.Dial(network, addr)
	}

	if err != nil {
		d.decrementCount(addr)
		return nil, err
	}

	if d.readTimeout > 0 {
		deadlineErr := netconn.SetDeadline(time.Now().Add(d.readTimeout))
		if deadlineErr != nil {
			d.decrementCount(addr)
			closeErr := netconn.Close()
			if closeErr != nil {
				return nil, fmt.Errorf("%s error during: %s", closeErr, deadlineErr)
			}
			return nil, deadlineErr
		}
	}

	c = &watchConn{netconn, func(c net.Conn, e error) {
		d.decrementCount(addr)
	}}

	return c, err
}

// DropEndpoint marks backend as dropped i.e. maintenance x
func (d *LimitDialer) DropEndpoint(endpoint string) {
	d.droppedEndpoint = endpoint
}

// NewLimitDialer returns new `LimitDialer`.
func NewLimitDialer(limit int64, readTimeout, dialTimeout time.Duration) *LimitDialer {
	return &LimitDialer{
		activeCons:  make(map[string]int64),
		limit:       limit,
		dialTimeout: dialTimeout,
		readTimeout: readTimeout,
	}
}
