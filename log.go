package mqtt

import "log/slog"

func slogErr(err error) slog.Attr {
	return slog.Any("error", err)
}
