package dial

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimitDialer(t *testing.T) {
	addr := "198.18.0.254:80"
	timeout := 10 * time.Millisecond
	dialer := NewLimitDialer(0, timeout, timeout)
	conn, err := dialer.Dial("tcp", addr)
	assert.NotNil(t, err, "")
	if !assert.Nil(t, conn) {
		defer func() {
			err := conn.Close()
			assert.Nil(t, err)
		}()
	}
}

func autoListener(t *testing.T) (net.Listener, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err.Error())
		return nil, ""
	}
	return listener, listener.Addr().String()
}

func TestLimitDialerMostLoadedEndpoint(t *testing.T) {
	timeout := time.Second

	l1, addr1 := autoListener(t)
	if l1 != nil {
		defer func() {
			err := l1.Close()
			assert.Nil(t, err)
		}()
	}

	l2, addr2 := autoListener(t)
	if l2 != nil {
		defer func() {
			err := l2.Close()
			assert.Nil(t, err)
		}()
	}

	dialer := NewLimitDialer(2, timeout, timeout)
	conn1, c1Err := dialer.Dial("tcp", addr1)
	if assert.NotNil(t, conn1) {
		defer func() {
			err := conn1.Close()
			assert.Nil(t, err)
		}()
	}
	assert.Nil(t, c1Err)
	conn2, c2Err := dialer.Dial("tcp", addr2)
	if assert.NotNil(t, conn2) {
		defer func() {
			err := conn2.Close()
			assert.Nil(t, err)
		}()
	}
	assert.Nil(t, c2Err)

	conn3, c3Err := dialer.Dial("tcp", addr2)
	if !assert.Nil(t, conn3) {
		defer func() {
			err := conn3.Close()
			assert.Nil(t, err)
		}()
	}
	assert.NotNil(t, c3Err)

}

func TestLimitDialerConcurrency(t *testing.T) {
	l, addr := autoListener(t)
	if l != nil {
		defer func() {
			err := l.Close()
			assert.Nil(t, err)
		}()
	}
	timeout := time.Second
	dialer := NewLimitDialer(4, timeout, timeout)
	gotErr := make(chan bool)
	for i := 0; i < 5; i++ {
		go func() {
			_, err := dialer.Dial("tcp", addr)
			if err != nil {
				gotErr <- true
			}
		}()
	}
	select {
	case e := <-gotErr:
		assert.True(t, e)
	case <-time.After(timeout):
		t.Error("At least one dial should return error")
	}
}
