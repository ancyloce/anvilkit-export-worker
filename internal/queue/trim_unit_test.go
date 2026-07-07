package queue

import (
	"testing"
	"time"
)

func TestMinIDAt(t *testing.T) {
	at := time.UnixMilli(1700000000123)
	if got := minIDAt(at); got != "1700000000123-0" {
		t.Errorf("minIDAt = %q", got)
	}
}

func TestMinStreamID(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"1000-0", "999-5", "999-5"},
		{"1000-0", "1000-1", "1000-0"},
		{"1000-2", "1000-2", "1000-2"},
		{"5-0", "1700000000123-0", "5-0"},
		{"garbage", "1000-0", "1000-0"}, // unparseable side loses
		{"1000-0", "garbage", "1000-0"},
	}
	for _, tc := range cases {
		if got := minStreamID(tc.a, tc.b); got != tc.want {
			t.Errorf("minStreamID(%q, %q) = %q, want %q", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestNextStreamID(t *testing.T) {
	cases := []struct{ in, want string }{
		{"0-0", "0-1"},
		{"1000-41", "1000-42"},
		{"1000", "1000-1"}, // bare ms means seq 0
		{"1000-18446744073709551615", "1001-0"},
	}
	for _, tc := range cases {
		if got := nextStreamID(tc.in); got != tc.want {
			t.Errorf("nextStreamID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
