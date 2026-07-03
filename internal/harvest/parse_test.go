package harvest

import (
	"reflect"
	"testing"
)

// TestExtractHTMLRefs covers every FR-009 tag form (EW-ARTIFACT-002 DoD).
func TestExtractHTMLRefs(t *testing.T) {
	html := []byte(`<!doctype html>
<html><head>
  <meta property="og:image" content="/assets/og.png">
  <meta name="twitter:image" content="/assets/tw.png">
  <link rel="stylesheet" href="/_next/static/css/main.css">
  <link rel="icon" href="/assets/favicon.ico">
  <script src="/_next/static/chunks/app.js" defer></script>
</head><body>
  <img src="/assets/hero.jpg" srcset="/assets/hero-640.jpg 640w, /assets/hero-1280.jpg 1280w">
  <picture>
    <source src="/assets/clip.mp4" srcset="/assets/pic-2x.webp 2x">
  </picture>
  <img src="/assets/hero.jpg">
</body></html>`)

	got := ExtractHTMLRefs(html)
	want := []string{
		"/assets/og.png",
		"/assets/tw.png",
		"/_next/static/css/main.css",
		"/assets/favicon.ico",
		"/_next/static/chunks/app.js",
		"/assets/hero.jpg",
		"/assets/hero-640.jpg",
		"/assets/hero-1280.jpg",
		"/assets/clip.mp4",
		"/assets/pic-2x.webp",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refs (document order, deduped):\ngot  %v\nwant %v", got, want)
	}
}

func TestExtractHTMLRefsDeterministic(t *testing.T) {
	html := []byte(`<img src="/a.png"><img src="/b.png"><img src="/a.png">`)
	first := ExtractHTMLRefs(html)
	second := ExtractHTMLRefs(html)
	if !reflect.DeepEqual(first, second) || len(first) != 2 {
		t.Fatalf("extraction must be deterministic and deduped: %v vs %v", first, second)
	}
}

func TestParseSrcset(t *testing.T) {
	got := ParseSrcset(" /a.jpg 640w, /b.jpg 2x ,/c.jpg ")
	want := []string{"/a.jpg", "/b.jpg", "/c.jpg"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseSrcset = %v, want %v", got, want)
	}
	if ParseSrcset("") != nil {
		t.Error("empty srcset must yield nothing")
	}
}

// TestExtractCSSURLs covers the §13.4 corpus (EW-ARTIFACT-003 DoD).
func TestExtractCSSURLs(t *testing.T) {
	css := []byte(`
@font-face { src: url("/fonts/inter.woff2") format("woff2"); }
.bg { background: url('/assets/bg.png') no-repeat; }
.rel { background-image: url(../img/rel.svg); }
.bare { cursor: url(/assets/cursor.cur), auto; }
.spaced { background: url(  "/assets/spaced.png"  ); }
.dup { background: url("/fonts/inter.woff2"); }
`)
	got := ExtractCSSURLs(css)
	want := []string{
		"/fonts/inter.woff2",
		"/assets/bg.png",
		"../img/rel.svg",
		"/assets/cursor.cur",
		"/assets/spaced.png",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("css urls:\ngot  %v\nwant %v", got, want)
	}
}
