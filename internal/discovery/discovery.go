package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const DefaultDiscoveryPort = 33222
const (
	discoveryTimeoutDefault = 750 * time.Millisecond
	announcementInterval    = 250 * time.Millisecond
	reverseRequestInterval  = 100 * time.Millisecond
	reverseRequestAttempts  = 3
	discoveryPacketSize     = 2048
	ipv4Len                 = 4
)

type Result struct {
	Instance        string
	OS              string
	Address         string
	Service         string
	Port            int
	HostFingerprint string
	PairingEnabled  bool
}

type packet struct {
	Type            string `json:"type"`
	Service         string `json:"service"`
	Name            string `json:"name,omitempty"`
	OS              string `json:"os,omitempty"`
	Port            int    `json:"port,omitempty"`
	CallbackPort    int    `json:"callback_port,omitempty"`
	HostFingerprint string `json:"host_cert_fingerprint,omitempty"`
	PairingEnabled  bool   `json:"pairing_enabled,omitempty"`
}

type Responder struct{ conn *net.UDPConn }

type AdvertiseOptions struct {
	OS               string
	HostFingerprint  string
	PairingEnabled   bool
	OnReverseConnect func(address string)
}

func Advertise(
	ctx context.Context,
	service, instance string,
	port int,
	opts AdvertiseOptions,
) (*Responder, error) {
	if strings.TrimSpace(service) == "" {
		return nil, errors.New("service is required")
	}

	if strings.TrimSpace(instance) == "" {
		host, err := os.Hostname()
		if err != nil || strings.TrimSpace(host) == "" {
			instance = "rmtx"
		} else {
			instance = host
		}
	}

	if strings.TrimSpace(opts.OS) == "" {
		opts.OS = runtime.GOOS
	}

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: DefaultDiscoveryPort})
	if err != nil {
		return nil, fmt.Errorf("listen for discovery: %w", err)
	}

	r := &Responder{conn: conn}
	go r.serve(ctx, service, instance, port, opts)
	go r.announce(ctx, service, instance, port, opts)

	return r, nil
}

func (r *Responder) Close() error {
	if r == nil || r.conn == nil {
		return nil
	}

	return r.conn.Close()
}

func (r *Responder) serve(
	ctx context.Context,
	service, instance string,
	port int,
	opts AdvertiseOptions,
) {
	defer func() { _ = r.conn.Close() }()

	go func() { <-ctx.Done(); _ = r.conn.Close() }()

	buf := make([]byte, discoveryPacketSize)
	for {
		n, addr, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var pkt packet
		if err := json.Unmarshal(buf[:n], &pkt); err != nil {
			continue
		}

		if pkt.Service != service {
			continue
		}

		if pkt.Type == "reverse_connect" {
			handleReverseConnect(pkt, addr, opts)

			continue
		}

		if pkt.Type != "query" {
			continue
		}

		response, err := responsePacket(service, instance, port, opts)
		if err != nil {
			continue
		}

		_, _ = r.conn.WriteToUDP(response, addr)
	}
}

func handleReverseConnect(pkt packet, addr *net.UDPAddr, opts AdvertiseOptions) {
	if opts.OnReverseConnect == nil || pkt.CallbackPort == 0 {
		return
	}

	if fingerprint := strings.TrimSpace(pkt.HostFingerprint); fingerprint != "" &&
		fingerprint != strings.TrimSpace(opts.HostFingerprint) {
		return
	}

	if addr == nil || addr.IP == nil {
		return
	}

	callback := net.JoinHostPort(addr.IP.String(), strconv.Itoa(pkt.CallbackPort))
	go opts.OnReverseConnect(callback)
}

func (r *Responder) announce(
	ctx context.Context,
	service, instance string,
	port int,
	opts AdvertiseOptions,
) {
	ticker := time.NewTicker(announcementInterval)
	defer ticker.Stop()

	for {
		if err := r.broadcastResponse(service, instance, port, opts); err != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Responder) broadcastResponse(
	service, instance string,
	port int,
	opts AdvertiseOptions,
) error {
	response, err := responsePacket(service, instance, port, opts)
	if err != nil {
		return err
	}

	for _, target := range broadcastTargets() {
		_, _ = r.conn.WriteToUDP(response, target)
	}

	return nil
}

func responsePacket(service, instance string, port int, opts AdvertiseOptions) ([]byte, error) {
	return json.Marshal(
		packet{
			Type:            "response",
			Service:         service,
			Name:            instance,
			OS:              opts.OS,
			Port:            port,
			HostFingerprint: opts.HostFingerprint,
			PairingEnabled:  opts.PairingEnabled,
		},
	)
}

func DiscoverOne(ctx context.Context, service string, timeout time.Duration) (Result, error) {
	results, err := discoverResults(ctx, service, timeout)
	if err != nil {
		return Result{}, err
	}

	if len(results) == 0 {
		return Result{}, fmt.Errorf("no host discovered via %s within %s", service, timeout)
	}

	return selectSingleResult(results)
}

func DiscoverAll(ctx context.Context, service string, timeout time.Duration) ([]Result, error) {
	if timeout <= 0 {
		timeout = discoveryTimeoutDefault
	}

	results, err := discoverResults(ctx, service, timeout)
	if err != nil {
		return nil, err
	}

	return orderedResults(results), nil
}

func discoverResults(
	ctx context.Context,
	service string,
	timeout time.Duration,
) (map[string]Result, error) {
	if timeout <= 0 {
		timeout = discoveryTimeoutDefault
	}

	conn, err := listenDiscovery()
	if err != nil {
		return nil, fmt.Errorf("listen for discovery responses: %w", err)
	}

	defer func() { _ = conn.Close() }()

	query, err := json.Marshal(packet{Type: "query", Service: service})
	if err != nil {
		return nil, err
	}

	for _, target := range broadcastTargets() {
		_, _ = conn.WriteToUDP(query, target)
	}

	return collectResponses(ctx, conn, service, timeout)
}

func listenDiscovery() (*net.UDPConn, error) {
	conn, err := net.ListenUDP(
		"udp4",
		&net.UDPAddr{IP: net.IPv4zero, Port: DefaultDiscoveryPort},
	)
	if err == nil {
		return conn, nil
	}

	return net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
}

func RequestReverseConnect(
	ctx context.Context,
	service string,
	hostAddress string,
	callbackPort int,
	hostFingerprint string,
) error {
	if callbackPort <= 0 {
		return errors.New("callback port is required")
	}

	host, _, err := net.SplitHostPort(hostAddress)
	if err != nil {
		return fmt.Errorf("parse host address %s: %w", hostAddress, err)
	}

	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err != nil {
		return fmt.Errorf("resolve host %s: %w", host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("resolve host %s: no IPv4 address", host)
	}

	conn, err := listenDiscovery()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	msg, err := json.Marshal(packet{
		Type:            "reverse_connect",
		Service:         service,
		CallbackPort:    callbackPort,
		HostFingerprint: strings.TrimSpace(hostFingerprint),
	})
	if err != nil {
		return err
	}

	target := &net.UDPAddr{IP: ips[0].To4(), Port: DefaultDiscoveryPort}

	for range reverseRequestAttempts {
		if _, err = conn.WriteToUDP(msg, target); err != nil {
			return err
		}

		timer := time.NewTimer(reverseRequestInterval)
		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
	}

	return nil
}

func orderedResults(results map[string]Result) []Result {
	ordered := make([]Result, 0, len(results))
	for _, r := range results {
		ordered = append(ordered, r)
	}

	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Address < ordered[j].Address })

	return ordered
}

func collectResponses(
	ctx context.Context,
	conn *net.UDPConn,
	service string,
	timeout time.Duration,
) (map[string]Result, error) {
	results := map[string]Result{}
	buf := make([]byte, discoveryPacketSize)

	deadline := time.Now().Add(timeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}

		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			shouldStop, readErr := handleReadError(ctx, err)
			if shouldStop {
				return results, nil
			}

			if readErr != nil {
				return nil, readErr
			}

			continue
		}

		recordResponse(results, service, buf[:n], addr)
	}
}

func handleReadError(ctx context.Context, err error) (bool, error) {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true, nil
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return false, fmt.Errorf("read discovery response: %w", err)
	}
}

func recordResponse(results map[string]Result, service string, raw []byte, addr *net.UDPAddr) {
	var pkt packet
	if err := json.Unmarshal(raw, &pkt); err != nil {
		return
	}

	if pkt.Type != "response" || pkt.Service != service || pkt.Port == 0 {
		return
	}

	address := net.JoinHostPort(addr.IP.String(), strconv.Itoa(pkt.Port))
	results[address] = Result{
		Instance:        pkt.Name,
		OS:              pkt.OS,
		Address:         address,
		Service:         pkt.Service,
		Port:            pkt.Port,
		HostFingerprint: pkt.HostFingerprint,
		PairingEnabled:  pkt.PairingEnabled,
	}
}

func selectSingleResult(results map[string]Result) (Result, error) {
	ordered := orderedResults(results)

	if len(ordered) > 1 {
		candidates := make([]string, 0, len(ordered))
		for _, r := range ordered {
			candidates = append(candidates, r.Address)
		}

		return Result{}, fmt.Errorf("multiple hosts discovered: %s", strings.Join(candidates, ", "))
	}

	return ordered[0], nil
}

func NormalizeAddress(hostport string, defaultPort int) string {
	if hostport == "" {
		return ""
	}

	if _, _, err := net.SplitHostPort(hostport); err == nil {
		return hostport
	}

	if strings.Contains(hostport, ":") {
		if ip := net.ParseIP(hostport); ip != nil && ip.To4() == nil {
			return net.JoinHostPort(hostport, strconv.Itoa(defaultPort))
		}
	}

	return net.JoinHostPort(hostport, strconv.Itoa(defaultPort))
}

func broadcastTargets() []*net.UDPAddr {
	out := map[string]*net.UDPAddr{
		net.JoinHostPort("255.255.255.255", strconv.Itoa(DefaultDiscoveryPort)): {
			IP:   net.IPv4bcast,
			Port: DefaultDiscoveryPort,
		},
	}

	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}

			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}

			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if !ok || ipNet.IP == nil || ipNet.IP.To4() == nil {
					continue
				}

				b := broadcastAddr(ipNet)
				if b == nil {
					continue
				}

				udp := &net.UDPAddr{IP: b, Port: DefaultDiscoveryPort}
				out[udp.String()] = udp
			}
		}
	}

	values := make([]*net.UDPAddr, 0, len(out))
	for _, v := range out {
		values = append(values, v)
	}

	return values
}

func broadcastAddr(ipNet *net.IPNet) net.IP {
	ip4 := ipNet.IP.To4()

	mask := net.IP(ipNet.Mask).To4()
	if ip4 == nil || mask == nil {
		return nil
	}

	b := make(net.IP, ipv4Len)
	for i := range 4 {
		b[i] = ip4[i] | ^mask[i]
	}

	return b
}
