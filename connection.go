package mqtt

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"
)

var ErrEmptyHost = errors.New("empty host")
var ErrUnsupportedScheme = errors.New("unsupported broker scheme")

type Factory interface {
	Create(broker string) (net.Conn, error)
}

type factory struct {
	dialer    *net.Dialer
	tlsConfig *tls.Config
	timeout   time.Duration
}

func NewFactory(dialer *net.Dialer, tlsConfig *tls.Config) Factory {
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}
	return &factory{dialer: dialer, tlsConfig: tlsConfig}
}

func newDefaultFactory() Factory {
	return NewFactory(nil, nil)
}

func (c *factory) Create(broker string) (net.Conn, error) {
	u, err := url.Parse(broker)
	if err != nil {
		return nil, fmt.Errorf("invalid broker url: %w", err)
	}
	if u.Host == "" {
		return nil, ErrEmptyHost
	}

	port, _ := strconv.Atoi(u.Port())
	if port == 0 {
		port = c.getDefaultPortForScheme(u.Scheme)
	}
	switch u.Scheme {
	case "tls":
		if c.dialer != nil {
			return tls.DialWithDialer(c.dialer, "tcp", c.getAddress(u.Hostname(), port), c.tlsConfig)
		}
		return tls.Dial("tcp", c.getAddress(u.Hostname(), port), c.tlsConfig)
	case "tcp":
		if c.dialer != nil {
			return c.dialer.Dial("tcp", c.getAddress(u.Hostname(), port))
		}
		return net.Dial("tcp", c.getAddress(u.Hostname(), port))
	default:
		return nil, ErrUnsupportedScheme
	}
}

func (_ *factory) getDefaultPortForScheme(scheme string) int {
	switch scheme {
	case "tls":
		return 8883
	default:
		return 1883
	}
}

func (_ *factory) getAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
