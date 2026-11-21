package bodystore

// A minimal JSON span scanner: it finds the byte range of every value without
// decoding or re-encoding anything, so a body can be cut into exact pieces.

type node struct {
	start, end int // value bytes are b[start:end]
	kind       byte
	key        string // object member name (raw, unescaped only if simple)
	children   []*node
}

type scanner struct {
	b   []byte
	err bool
}

func (s *scanner) ws(i int) int {
	for i < len(s.b) {
		switch s.b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func (s *scanner) str(i int) int { // b[i] == '"'; returns index after closing quote
	i++
	for i < len(s.b) {
		switch s.b[i] {
		case '\\':
			i += 2
		case '"':
			return i + 1
		default:
			i++
		}
	}
	s.err = true
	return len(s.b)
}

// parse returns the node for the value starting at or after i, building
// children down to maxDepth (deeper containers are scanned but not kept).
func (s *scanner) parse(i, depth, maxDepth int) *node {
	i = s.ws(i)
	if depth > 512 {
		s.err = true
		return &node{start: i, end: len(s.b)}
	}
	if i >= len(s.b) {
		s.err = true
		return &node{start: i, end: i}
	}
	n := &node{start: i, kind: s.b[i]}
	switch s.b[i] {
	case '"':
		n.end = s.str(i)
	case '{', '[':
		close := byte('}')
		if s.b[i] == '[' {
			close = ']'
		}
		i++
		for {
			i = s.ws(i)
			if i < len(s.b) && s.b[i] == close {
				n.end = i + 1
				return n
			}
			key := ""
			if n.kind == '{' {
				if i >= len(s.b) || s.b[i] != '"' {
					s.err = true
					n.end = len(s.b)
					return n
				}
				ke := s.str(i)
				if s.err {
					n.end = len(s.b)
					return n
				}
				key = string(s.b[i+1 : ke-1])
				i = s.ws(ke)
				if i >= len(s.b) || s.b[i] != ':' {
					s.err = true
					n.end = len(s.b)
					return n
				}
				i++
			}
			c := s.parse(i, depth+1, maxDepth)
			c.key = key
			if depth < maxDepth {
				n.children = append(n.children, c)
			}
			i = s.ws(c.end)
			if s.err || i >= len(s.b) {
				s.err = true
				n.end = len(s.b)
				return n
			}
			if s.b[i] == ',' {
				i++
				continue
			}
			if s.b[i] == close {
				n.end = i + 1
				return n
			}
			s.err = true
			n.end = len(s.b)
			return n
		}
	default: // number, true, false, null
		for i < len(s.b) {
			switch s.b[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				n.end = i
				return n
			}
			i++
		}
		n.end = i
	}
	return n
}

// parseJSON returns the root node, or nil when b is not one JSON value.
func parseJSON(b []byte, maxDepth int) *node {
	s := &scanner{b: b}
	root := s.parse(0, 0, maxDepth)
	if s.err || s.ws(root.end) != len(b) {
		return nil
	}
	return root
}
