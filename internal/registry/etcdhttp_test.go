package registry

import "testing"

func TestGetPrefixRangeEnd(t *testing.T) {
	cases := []struct {
		prefix string
		want   string
	}{
		{"/sharding/", "/sharding0"},
		{"a", "b"},
		{"", "\x00"},
	}
	for _, tc := range cases {
		got := getPrefixRangeEnd(tc.prefix)
		if got != tc.want {
			t.Errorf("getPrefixRangeEnd(%q) = %q, want %q", tc.prefix, got, tc.want)
		}
	}
}

func TestGetPrefixRangeEnd_AllFF(t *testing.T) {
	got := getPrefixRangeEnd(string([]byte{0xff, 0xff}))
	if got != "\x00" {
		t.Errorf("expected fallback \\x00, got %q", got)
	}
}
