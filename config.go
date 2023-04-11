package mqtt

import (
	"golang.org/x/exp/slog"
	"time"
)

type config struct {
	clientId                  string
	broker                    string
	username                  string
	password                  string
	cleanSession              bool
	connectTimeout            time.Duration
	connectBackoffMaxInterval time.Duration
	disconnectTimeout         time.Duration
	keepAliveInterval         time.Duration
	keepAliveTimeout          time.Duration

	onConnectionStatusHandler OnConnectionStatusHandler
	onReceiveHandler          OnReceiveHandler

	connectionFactory Factory
	logger            *slog.Logger
}

type ClientOption func(*client)

func WithClientId(clientId string) ClientOption {
	return func(c *client) {
		c.config.clientId = clientId
	}
}

func WithUsername(username string) ClientOption {
	return func(c *client) {
		c.config.username = username
	}
}

func WithPassword(password string) ClientOption {
	return func(c *client) {
		c.config.password = password
	}
}

func WithCleanSession(clean bool) ClientOption {
	return func(c *client) {
		c.config.cleanSession = clean
	}
}

func WithConnectTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.config.connectTimeout = timeout
	}
}

func WithConnectBackoffMaxInterval(maxInterval time.Duration) ClientOption {
	return func(c *client) {
		c.config.connectBackoffMaxInterval = maxInterval
	}
}

func WithDisconnectTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.config.disconnectTimeout = timeout
	}
}

func WithKeepAliveInterval(interval time.Duration) ClientOption {
	return func(c *client) {
		c.config.keepAliveInterval = interval
	}
}

func WithKeepAliveTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.config.keepAliveTimeout = timeout
	}
}

func WithOnConnectionStatusHandler(handler OnConnectionStatusHandler) ClientOption {
	return func(c *client) {
		c.config.onConnectionStatusHandler = handler
	}
}

func WithConnectionFactory(factory Factory) ClientOption {
	return func(c *client) {
		c.config.connectionFactory = factory
	}
}

func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *client) {
		c.config.logger = logger
	}
}
