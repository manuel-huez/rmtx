package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const DefaultDiscoveryPort = 33222

type Result struct {
	Instance string
	Address  string
	Port     int
}

type packet struct {
	Type    string `json:"type"`
	Service string `json:"service"`
	Name    string `json:"name,omitempty"`
	Port    int    `json:"port,omitempty"`
}

type Responder struct{ conn *net.UDPConn }

func Advertise(
	ctx context.Context,
	service, instance string,
	port int,
	_ []string,
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

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: DefaultDiscoveryPort})
	if err != nil {
		return nil, fmt.Errorf("listen for discovery: %w", err)
	}

	r := &Responder{conn: conn}
	go r.serve(ctx, service, instance, port)

	return r, nil
}

func (r *Responder) Close() error {
	if r == nil || r.conn == nil {
		return nil
	}

	return r.conn.Close()
}

func (r *Responder) serve(ctx context.Context, service, instance string, port int) {
	defer r.conn.Close()

	go func() { <-ctx.Done(); _ = r.conn.Close() }()

	buf := make([]byte, 2048)
	for {
		n, addr, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var pkt packet
		if err := json.Unmarshal(buf[:n], &pkt); err != nil {
			continue
		}

		if pkt.Type != "query" || pkt.Service != service {
			continue
		}

		response, err := json.Marshal(
			packet{Type: "response", Service: service, Name: instance, Port: port},
		)
		if err != nil {
			continue
		}

		_, _ = r.conn.WriteToUDP(response, addr)
	}
}

func DiscoverOne(ctx context.Context, service string, timeout time.Duration) (Result, error) {
	if timeout <= 0 {
		timeout = 750 * time.Millisecond
	}

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return Result{}, fmt.Errorf("listen for discovery responses: %w", err)
	}
	defer conn.Close()

	query, err := json.Marshal(packet{Type: "query", Service: service})
	if err != nil {
		return Result{}, err
	}

	for _, target := range broadcastTargets() {
		_, _ = conn.WriteToUDP(query, target)
	}

	results := map[string]Result{}
	buf := make([]byte, 2048)

	deadline := time.Now().Add(timeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return Result{}, err
		}

		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				break
			}

			select {
			case <-ctx.Done():
				return Result{}, ctx.Err()
			default:
			}

			return Result{}, fmt.Errorf("read discovery response: %w", err)
		}

		var pkt packet
		if err := json.Unmarshal(buf[:n], &pkt); err != nil {
			continue
		}

		if pkt.Type != "response" || pkt.Service != service || pkt.Port == 0 {
			continue
		}

		address := net.JoinHostPort(addr.IP.String(), strconv.Itoa(pkt.Port))
		results[address] = Result{Instance: pkt.Name, Address: address, Port: pkt.Port}
	}

	if len(results) == 0 {
		return Result{}, fmt.Errorf("no host discovered via %s within %s", service, timeout)
	}

	ordered := make([]Result, 0, len(results))
	for _, r := range results {
		ordered = append(ordered, r)
	}

	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Address < ordered[j].Address })

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

	b := make(net.IP, 4)
	for i := range 4 {
		b[i] = ip4[i] | ^mask[i]
	}

	return b
}
