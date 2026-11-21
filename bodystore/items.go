package bodystore

type span struct{ start, end int }

// Protocol item containers: every element is one unit. system is an array of
// blocks in Claude, a string elsewhere; non-array values are one unit.
var itemArrays = map[string]bool{"messages": true, "input": true, "contents": true, "tools": true, "system": true}
var itemValues = map[string]bool{"system": true, "instructions": true, "systemInstruction": true, "tools": true}

func itemNodes(body []byte, depth int) []*node {
	root := parseJSON(body, depth)
	if root == nil || root.kind != '{' {
		return nil
	}
	var out []*node
	for _, c := range root.children {
		switch {
		case itemArrays[c.key] && c.kind == '[':
			out = append(out, c.children...)
		case itemValues[c.key]:
			out = append(out, c)
		}
	}
	return out
}

// item: one blob per protocol item and per system/instructions value.
func itemSpans(body []byte) []span {
	nodes := itemNodes(body, 2)
	if nodes == nil {
		return wholeBody(body)
	}
	out := make([]span, 0, len(nodes))
	for _, it := range nodes {
		out = append(out, span{it.start, it.end})
	}
	return out
}

func wholeBody(body []byte) []span {
	if len(body) == 0 {
		return nil
	}
	return []span{{0, len(body)}}
}
