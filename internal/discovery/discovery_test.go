//nolint:goconst // Repeated fixture literals keep each test case self-contained.
package discovery

import (
	"net"
	"testing"
	"time"
)

func TestResponderDedupesReverseConnectRequests(t *testing.T) {
	r := &Responder{reverseSlots: make(chan struct{}, maxReverseConnections)}
	defer r.wg.Wait()

	callbacks := make(chan string, 2)

	pkt := packet{
		CallbackPort:    64595,
		RequestID:       "request-1",
		HostFingerprint: "host-fingerprint",
	}
	addr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 33222}
	opts := AdvertiseOptions{
		HostFingerprint: "host-fingerprint",
		OnReverseConnect: func(address string) {
			callbacks <- address
		},
	}

	r.handleReverseConnect(pkt, addr, opts)
	r.handleReverseConnect(pkt, addr, opts)

	select {
	case got := <-callbacks:
		want := "192.0.2.10:64595"
		if got != want {
			t.Fatalf("unexpected callback address: got %s want %s", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("expected reverse connect callback")
	}

	select {
	case got := <-callbacks:
		t.Fatalf("unexpected duplicate reverse connect callback: %s", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReverseRequestKeyRejectsMissingRequestID(t *testing.T) {
	pkt := packet{HostFingerprint: "host-fingerprint"}

	got := reverseRequestKey(pkt)
	if got != "" {
		t.Fatalf("reverse request key = %q, want rejection", got)
	}
}

func TestReverseRequestKeyUsesRequestID(t *testing.T) {
	pkt := packet{RequestID: "request-1", HostFingerprint: "host-fingerprint"}

	got := reverseRequestKey(pkt)
	want := "host-fingerprint\x00id\x00request-1"

	if got != want {
		t.Fatalf("unexpected reverse request key: got %q want %q", got, want)
	}
}
