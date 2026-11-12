package tap

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestCaddyfileAdapter(t *testing.T) {
	cfg := `http://localhost:8080 {
		@api path /api/*
		tap @api {
			identity main-api
			directory "/tmp/tap logs"
			mode strict
			queue_size 3
			roll_size 10MiB
			roll_interval 1d
			roll_keep -1
			omit_fields identity request.header request.header_base64
			roll_keep_for -1
		}
		handle /other/* {
			route {
				tap {
					identity other
					directory /tmp/other
					max_body_bytes 1KiB
				}
				respond "other"
			}
		}
	}`
	adapted, _, err := caddyconfig.GetAdapter("caddyfile").Adapt([]byte(cfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	type route struct {
		Match  []map[string][]string
		Handle []json.RawMessage
	}
	var tree struct {
		Apps struct {
			HTTP struct {
				Servers map[string]struct{ Routes []route }
			}
		}
	}
	if err := json.Unmarshal(adapted, &tree); err != nil {
		t.Fatal(err)
	}
	var site struct{ Routes []route }
	outer := tree.Apps.HTTP.Servers["srv0"].Routes
	if len(outer) != 1 || len(outer[0].Handle) != 1 {
		t.Fatalf("unexpected site structure %s", adapted)
	}
	if err := json.Unmarshal(outer[0].Handle[0], &site); err != nil {
		t.Fatal(err)
	}
	routes := site.Routes
	if len(routes) != 2 || len(routes[0].Match) != 1 || len(routes[0].Match[0]["path"]) != 1 ||
		routes[0].Match[0]["path"][0] != "/api/*" || !strings.Contains(string(routes[1].Handle[0]), `"identity":"other"`) {
		t.Fatalf("unexpected route structure %s", adapted)
	}
	var tap Tap
	if err := json.Unmarshal(routes[0].Handle[0], &tap); err != nil {
		t.Fatal(err)
	}
	if tap.Identity != "main-api" || tap.Directory != "/tmp/tap logs" || tap.Mode != "strict" ||
		tap.QueueSize != 3 || tap.RollSize != 10<<20 || time.Duration(tap.RollInterval) != 24*time.Hour ||
		tap.RollKeep != -1 || tap.RollKeepFor != -1 ||
		!slices.Equal(tap.OmitFields, []string{"identity", "request.header", "request.header_base64"}) {
		t.Fatalf("unexpected config %+v", tap)
	}
}

func TestInvalidCaddyfile(t *testing.T) {
	d := caddyfile.NewTestDispenser("tap unexpected")
	if err := new(Tap).UnmarshalCaddyfile(d); err == nil || !strings.Contains(err.Error(), "wrong argument") {
		t.Fatalf("unexpected directive argument: %v", err)
	}
	for _, tc := range []struct{ option, want string }{
		{"identity second", "duplicate"},
		{"bogus true", "unknown"},
		{"mode discard", "drop or strict"},
		{"mode", "wrong argument"},
		{"mode drop {\nqueue_size 1\n}", "nested"},
		{"roll_size 0", "positive"},
		{"roll_size 9EiB", "fit in int64"},
		{"roll_size nope", "roll_size"},
		{"max_body_bytes 4MiB extra", "wrong argument"},
		{"roll_interval -2h", "nonnegative"},
		{"roll_keep -2", "retention"},
		{"roll_keep_for -1h", "nonnegative"},
		{"queue_size 0", "positive"},
		{"queue_size 65537", "65536"},
		{"mode strict\nmax_body_bytes 1KiB", "strict mode"},
		{"omit_fields", "wrong argument"},
		{"omit_fields identity\nomit_fields request.header", "duplicate"},
		{"omit_fields request..header", "empty segment"},
		{"omit_fields identity {\nrequest.header\n}", "nested"},
		{`omit_fields ""`, "empty segment"},
	} {
		t.Run(tc.option, func(t *testing.T) {
			d := caddyfile.NewTestDispenser("tap {\nidentity api\ndirectory /tmp\n" + tc.option + "\n}")
			if err := new(Tap).UnmarshalCaddyfile(d); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
	for _, cfg := range []Tap{{Directory: "/tmp"}, {Identity: "../bad", Directory: "/tmp"}, {Identity: "valid"}, {Identity: "valid", Directory: "/tmp", RollKeepFor: -2}} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("accepted invalid JSON config %+v", cfg)
		}
	}
}

func TestOmitFieldsJSONConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{`[]`, true},
		{`["identity", "request.header.User-Agent"]`, true},
		{`[null]`, false},
		{`[""]`, false},
		{`[".identity"]`, false},
		{`["identity."]`, false},
		{`["request. .header"]`, false},
		{`"identity"`, false},
		{`[42]`, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			var cfg Tap
			err := json.Unmarshal([]byte(`{"identity":"api","directory":"/tmp","omit_fields":`+tc.value+`}`), &cfg)
			if err == nil {
				err = cfg.Validate()
			}
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t, got %v", tc.valid, err)
			}
		})
	}
}

func readmeExamples(t *testing.T, filename, language string) []string {
	t.Helper()
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	var examples []string
	rest := string(content)
	for {
		_, code, found := strings.Cut(rest, "```"+language+"\n")
		if !found {
			break
		}
		example, tail, closed := strings.Cut(code, "\n```")
		if !closed {
			t.Fatalf("%s %s example has no closing fence", filename, language)
		}
		examples = append(examples, example)
		rest = tail
	}
	if len(examples) == 0 {
		t.Fatalf("%s has no %s example", filename, language)
	}
	return examples
}

func TestReadmeJSONExample(t *testing.T) {
	for _, example := range readmeExamples(t, "README.md", "json") {
		var cfg struct {
			Handler string `json:"handler"`
			Tap
		}
		d := json.NewDecoder(strings.NewReader(example))
		d.DisallowUnknownFields()
		if err := d.Decode(&cfg); err != nil {
			t.Fatalf("README JSON: %v", err)
		}
		if err := cfg.Tap.Validate(); err != nil || cfg.Handler != "tap" {
			t.Fatalf("README handler %q, config error %v", cfg.Handler, err)
		}
	}
}

func TestReadmeExamplesAgree(t *testing.T) {
	for _, language := range []string{"caddyfile", "json"} {
		english := readmeExamples(t, "README.md", language)
		chinese := readmeExamples(t, "README.zh-CN.md", language)
		if !slices.Equal(english, chinese) {
			t.Errorf("README %s examples differ", language)
		}
	}
}
