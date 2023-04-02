package mqtt

import "golang.org/x/exp/slog"

func slogErr(err error) slog.Attr {
	return slog.Any("error", err)
}
