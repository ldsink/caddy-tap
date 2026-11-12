package tap

import (
	"fmt"
	"math"
	"strconv"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dustin/go-humanize"
)

// UnmarshalCaddyfile parses one tap directive after Caddy resolves its matcher.
func (t *Tap) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	if d.NextArg() {
		return d.ArgErr()
	}
	seen := make(map[string]bool)
	for d.NextBlock(0) {
		if d.Nesting() != 1 {
			return d.Errf("nested tap options are unsupported")
		}
		option := d.Val()
		if seen[option] {
			return d.Errf("duplicate tap option %q", option)
		}
		seen[option] = true
		if !d.NextArg() {
			return d.ArgErr()
		}
		value := d.Val()
		if option == "omit_fields" {
			t.OmitFields = append([]string{value}, d.RemainingArgs()...)
			continue
		}
		if d.NextArg() {
			return d.ArgErr()
		}
		var err error
		switch option {
		case "identity":
			t.Identity = value
		case "directory":
			t.Directory = value
		case "mode":
			t.Mode = value
		case "queue_size":
			t.QueueSize, err = strconv.Atoi(value)
			if err == nil && t.QueueSize <= 0 {
				err = fmt.Errorf("must be positive")
			}
		case "max_body_bytes":
			t.MaxBodyBytes, err = parseSize(value)
		case "roll_size":
			t.RollSize, err = parseSize(value)
		case "roll_interval", "roll_keep_for":
			duration, parseErr := caddy.ParseDuration(value)
			err = parseErr
			if option == "roll_interval" {
				t.RollInterval = caddy.Duration(duration)
			} else if value == "-1" {
				t.RollKeepFor, err = -1, nil
			} else {
				t.RollKeepFor = caddy.Duration(duration)
				if err == nil && duration < 0 {
					err = fmt.Errorf("must be nonnegative or -1")
				}
			}
		case "roll_keep":
			t.RollKeep, err = strconv.Atoi(value)
		default:
			return d.Errf("unknown tap option %q", option)
		}
		if err != nil {
			return d.Errf("%s: %v", option, err)
		}
	}
	if err := t.Validate(); err != nil {
		return d.Errf("%v", err)
	}
	return nil
}

func parseSize(value string) (int64, error) {
	size, err := humanize.ParseBytes(value)
	if err != nil {
		return 0, err
	}
	if size == 0 || size > math.MaxInt64 {
		return 0, fmt.Errorf("size must be positive and fit in int64")
	}
	return int64(size), nil
}
