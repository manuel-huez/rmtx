package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/manuel-huez/rmtx/internal/discovery"
	"github.com/manuel-huez/rmtx/internal/protocol"
	"github.com/manuel-huez/rmtx/internal/security"
)

const (
	directDialTimeout    = 1500 * time.Millisecond
	reverseFallbackDelay = 50 * time.Millisecond
	reverseDialTimeout   = 5 * time.Second
	defaultDiscoverySvc  = "rmtx"
	tcpKeepAliveEvery    = 15 * time.Second
)

func dialTLS(
	ctx context.Context,
	address, discoveryService, fingerprint string,
	clientCertPEM, clientKeyPEM []byte,
) (*protocol.Conn, error) {
	tlsConfig, err := security.ClientTLSConfig(clientCertPEM, clientKeyPEM, fingerprint)
	if err != nil {
		return nil, err
	}

	return dialFastestTLS(ctx, address, discoveryService, fingerprint, tlsConfig)
}

type dialTLSResult struct {
	conn *protocol.Conn
	err  error
}

func dialFastestTLS(
	ctx context.Context,
	address, discoveryService, fingerprint string,
	tlsConfig *tls.Config,
) (*protocol.Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	directCtx, directCancel := context.WithTimeout(ctx, directDialTimeout)
	defer func() {
		directCancel()
		cancel()
		close(done)
	}()

	directCh := startDialTLS(done, func() (*protocol.Conn, error) {
		return dialDirectTLS(directCtx, address, tlsConfig)
	})

	// Reverse starts shortly after direct so firewalled local dials skip the full direct timeout.
	timer := time.NewTimer(reverseFallbackDelay)
	defer timer.Stop()
	timerCh := timer.C

	var (
		reverseCh  <-chan dialTLSResult
		directErr  error
		reverseErr error
	)

	for directCh != nil || reverseCh != nil {
		select {
		case result := <-directCh:
			directCh = nil
			if result.err == nil {
				return result.conn, nil
			}
			directErr = result.err
			if reverseCh == nil {
				stopTimer(timer)
				timerCh = nil
				reverseCh = startReverseDialTLS(ctx, done, address, discoveryService, fingerprint, tlsConfig)
			}
		case <-timerCh:
			timerCh = nil
			if reverseCh == nil {
				reverseCh = startReverseDialTLS(ctx, done, address, discoveryService, fingerprint, tlsConfig)
			}
		case result := <-reverseCh:
			reverseCh = nil
			if result.err == nil {
				return result.conn, nil
			}
			reverseErr = result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf(
		"dial host %s: %w; reverse connect failed: %w",
		address,
		directErr,
		reverseErr,
	)
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func startReverseDialTLS(
	ctx context.Context,
	done <-chan struct{},
	address, discoveryService, fingerprint string,
	tlsConfig *tls.Config,
) <-chan dialTLSResult {
	return startDialTLS(done, func() (*protocol.Conn, error) {
		return dialReverseTLS(
			ctx,
			address,
			nonEmpty(discoveryService, defaultDiscoverySvc),
			fingerprint,
			tlsConfig,
		)
	})
}

func startDialTLS(
	done <-chan struct{},
	dial func() (*protocol.Conn, error),
) <-chan dialTLSResult {
	ch := make(chan dialTLSResult)
	go func() {
		conn, err := dial()
		result := dialTLSResult{conn: conn, err: err}

		select {
		case ch <- result:
		case <-done:
			if conn != nil {
				closeQuietly(conn.Raw())
			}
		}
	}()

	return ch
}

func dialDirectTLS(
	ctx context.Context,
	address string,
	tlsConfig *tls.Config,
) (*protocol.Conn, error) {
	dialer := net.Dialer{KeepAlive: tcpKeepAliveEvery}
	raw, err := (&tls.Dialer{NetDialer: &dialer, Config: tlsConfig}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}

	return protocol.NewConn(protocol.NewIdleDeadlineConn(raw)), nil
}

func dialReverseTLS(
	ctx context.Context,
	address, discoveryService, fingerprint string,
	tlsConfig *tls.Config,
) (*protocol.Conn, error) {
	listenConfig := net.ListenConfig{KeepAlive: tcpKeepAliveEvery}
	ln, err := listenConfig.Listen(ctx, "tcp4", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen for reverse connection: %w", err)
	}
	defer closeQuietly(ln)

	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || tcpAddr.Port == 0 {
		return nil, fmt.Errorf("listen for reverse connection: unexpected address %s", ln.Addr())
	}

	ctx, cancel := context.WithTimeout(ctx, reverseDialTimeout)
	defer cancel()

	requestCtx, requestCancel := context.WithCancel(ctx)
	defer requestCancel()

	requestErrCh := startReverseConnectRequests(
		requestCtx,
		discoveryService,
		address,
		tcpAddr.Port,
		fingerprint,
	)

	raw, err := acceptReverse(ctx, ln, requestErrCh)
	if err != nil {
		return nil, err
	}

	requestCancel()

	conn := tls.Client(raw, tlsConfig)
	if err := conn.HandshakeContext(ctx); err != nil {
		closeQuietly(conn)

		return nil, fmt.Errorf("reverse tls handshake: %w", err)
	}

	return protocol.NewConn(protocol.NewIdleDeadlineConn(conn)), nil
}

func startReverseConnectRequests(
	ctx context.Context,
	discoveryService string,
	address string,
	callbackPort int,
	fingerprint string,
) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- discovery.RequestReverseConnect(
			ctx,
			discoveryService,
			address,
			callbackPort,
			fingerprint,
		)
	}()

	return errCh
}

func nonEmpty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}

	return value
}

func acceptReverse(
	ctx context.Context,
	ln net.Listener,
	requestErrCh <-chan error,
) (net.Conn, error) {
	type acceptResult struct {
		conn net.Conn
		err  error
	}

	resultCh := make(chan acceptResult, 1)

	go func() {
		conn, err := ln.Accept()
		resultCh <- acceptResult{conn: conn, err: err}
	}()

	for {
		select {
		case <-ctx.Done():
			_ = ln.Close()

			return nil, fmt.Errorf("accept reverse connection: %w", ctx.Err())
		case err := <-requestErrCh:
			requestErrCh = nil

			if err != nil {
				_ = ln.Close()

				return nil, err
			}
		case result := <-resultCh:
			if result.err != nil {
				return nil, fmt.Errorf("accept reverse connection: %w", result.err)
			}

			return result.conn, nil
		}
	}
}
