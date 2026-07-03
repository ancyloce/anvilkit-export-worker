package harvest

import (
	"bytes"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

// ExtractHTMLRefs collects every dependency reference from the rendered HTML
// in document order, deduplicated (FR-009 exhaustive tag set): <link href>,
// <script src>, <img src>, <source src>, srcset, and
// og:image / twitter:image meta content.
func ExtractHTMLRefs(body []byte) []string {
	var refs []string
	seen := map[string]bool{}
	add := func(ref string) {
		ref = strings.TrimSpace(ref)
		if ref == "" || seen[ref] {
			return
		}
		seen[ref] = true
		refs = append(refs, ref)
	}

	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	for {
		tokenType := tokenizer.Next()
		if tokenType == html.ErrorToken {
			return refs // io.EOF or malformed tail — collect what parsed
		}
		if tokenType != html.StartTagToken && tokenType != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		attrs := map[string]string{}
		for _, a := range token.Attr {
			attrs[strings.ToLower(a.Key)] = a.Val
		}
		switch token.Data {
		case "link":
			add(attrs["href"])
		case "script":
			add(attrs["src"])
		case "img", "source":
			add(attrs["src"])
			for _, candidate := range ParseSrcset(attrs["srcset"]) {
				add(candidate)
			}
		case "meta":
			if strings.EqualFold(attrs["property"], "og:image") ||
				strings.EqualFold(attrs["name"], "twitter:image") {
				add(attrs["content"])
			}
		}
	}
}

// ParseSrcset extracts the URL of every srcset candidate ("url 2x, url2 640w").
func ParseSrcset(srcset string) []string {
	var out []string
	for _, candidate := range strings.Split(srcset, ",") {
		fields := strings.Fields(strings.TrimSpace(candidate))
		if len(fields) > 0 {
			out = append(out, fields[0])
		}
	}
	return out
}

var cssURLRe = regexp.MustCompile(`url\(\s*(?:'([^']*)'|"([^"]*)"|([^'")\s]+))\s*\)`)

// ExtractCSSURLs collects url(...) references from a downloaded stylesheet
// in document order, deduplicated (FR-009 / PRD 0008 §13.4).
func ExtractCSSURLs(css []byte) []string {
	var refs []string
	seen := map[string]bool{}
	for _, match := range cssURLRe.FindAllSubmatch(css, -1) {
		var ref string
		for _, group := range match[1:] {
			if len(group) > 0 {
				ref = string(group)
				break
			}
		}
		ref = strings.TrimSpace(ref)
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	return refs
}
